package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/NebulousLabs/errors"
	"github.com/NebulousLabs/fastrand"
	"github.com/NebulousLabs/writeaheadlog"
	"golang.org/x/crypto/blake2b"
)

func main() {
	testdir := "./test"
	dbPath := filepath.Join(testdir, "database.dat")
	walPath := filepath.Join(testdir, "wal.dat")

	// Create the test dir
	os.MkdirAll(testdir, 0777)
	defer os.RemoveAll(testdir)

	// Declare some vars to configure the loop
	var wg sync.WaitGroup
	numSilos := int64(250)
	numIncrease := 20

	// Create new fake database file or open it
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		panic(err)
	}

	// Create/Recover WAL
	updates, wal, err := writeaheadlog.New(walPath)
	if err != nil {
		panic(err)
	}

	// Create silo objects
	var siloOff int64
	var siloOffsets []int64
	var silos = make(map[int64]*silo)
	for i := 0; int64(i) < numSilos; i++ {
		silo := newSilo(siloOff, 1+i*numIncrease, dbFile)
		siloOffsets = append(siloOffsets, siloOff)
		silos[siloOff] = silo
		siloOff += int64(len(silo.numbers)*4) + checksumSize
	}

	// Start silo updates if the wal didn't exist
	if len(updates) == 0 {
		for k := range silos {
			if err := silos[k].init(); err != nil {
				panic(fmt.Sprintf("Failed to init silo: %v", err))
			}

			wg.Add(1)
			go silos[k].threadedUpdate(wal, testdir, &wg)
		}

		// Signal that PC can be unplugged now
		log.Print("UNPLUG!!!1!!1111!!!")

		// Wait for all the threads to fail
		wg.Wait()

		// This should never be reached
		panic("This should never be reached")
	}

	// WAL exists. Start recovery
	log.Print("len(updates)>0: Starting recovery. Feel free to UNPLUG again")
	err = recoverSiloWAL(walPath, silos, testdir, dbFile, numSilos, numIncrease)
	if err != nil {
		log.Printf("Recovery failed: %v", err)
		return
	}
	log.Printf("Recovery was successful. Have a nice day! :)")
}

const (
	checksumSize = 16
	pageSize     = 4096
)

type (
	// A checksum is a 128-bit blake2b hash.
	checksum [checksumSize]byte

	silo struct {
		// list of numbers in silo and the index of the next number that will
		// be incremented
		numbers    []uint32
		nextNumber uint32

		f      *os.File // database file holding silo
		offset int64    // offset within database
	}

	siloUpdate struct {
		// offset is the in the file at which the number should be written
		offset int64

		// number is the number which is written at offset
		number uint32

		// silo is the offset of the silo so we can apply the update to the right silo
		silo int64

		// oldChecksum is the checksum of the file that became obsolete after changing a
		// number. It needs to be removed when the update is applied.
		oldChecksum checksum

		// newChecksum is the new checkum of the silo and the corresponding file
		newChecksum checksum

		// ncso is the offset of the new checksum
		newChecksumOffset int64
	}
)

// newSilo creates a new silo of a certain length at a specific offset in the
// file
func newSilo(offset int64, length int, f *os.File) *silo {
	if length == 0 {
		panic("numbers shouldn't be empty")
	}

	return &silo{
		offset:  offset,
		numbers: make([]uint32, length, length),
		f:       f,
	}
}

// marshal marshals a siloUpdate
func (su siloUpdate) marshal() []byte {
	data := make([]byte, 28+2*checksumSize)
	binary.LittleEndian.PutUint64(data[0:8], uint64(su.offset))
	binary.LittleEndian.PutUint32(data[8:12], su.number)
	binary.LittleEndian.PutUint64(data[12:20], uint64(su.silo))
	binary.LittleEndian.PutUint64(data[20:28], uint64(su.newChecksumOffset))
	copy(data[28:28+checksumSize], su.oldChecksum[:])
	copy(data[28+checksumSize:], su.newChecksum[:])
	return data
}

// unmarshal unmarshals a siloUpdate from data
func (su *siloUpdate) unmarshal(data []byte) {
	if len(data) != 28+2*checksumSize {
		panic("data has wrong size")
	}
	su.offset = int64(binary.LittleEndian.Uint64(data[0:8]))
	su.number = binary.LittleEndian.Uint32(data[8:12])
	su.silo = int64(binary.LittleEndian.Uint64(data[12:20]))
	su.newChecksumOffset = int64(binary.LittleEndian.Uint64(data[20:28]))
	copy(su.oldChecksum[:], data[28:28+checksumSize])
	copy(su.newChecksum[:], data[28+checksumSize:])
	return
}

// newUpdate create a WAL update from a siloUpdate
func (su *siloUpdate) newUpdate() writeaheadlog.Update {
	update := writeaheadlog.Update{
		Name:         "This is my update. There are others like it but this one is mine",
		Version:      "v0.9.8.7.6.5.4.3.2.1.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z",
		Instructions: su.marshal(),
	}
	return update
}

// newSiloUpdate creates a new Silo update for a number at a specific index
func (s *silo) newSiloUpdate(index uint32, number uint32, ocs checksum) *siloUpdate {
	return &siloUpdate{
		number:            number,
		offset:            s.offset + int64(4*(index)),
		silo:              s.offset,
		oldChecksum:       ocs,
		newChecksumOffset: s.offset + int64(len(s.numbers)*4),
	}
}

// checksum calculates the silos's current checksum
func (s *silo) checksum() (cs checksum) {
	buf := make([]byte, 4*len(s.numbers))
	for i := 0; i < len(s.numbers); i++ {
		binary.LittleEndian.PutUint32(buf[i*4:i*4+4], s.numbers[i])
	}
	c := blake2b.Sum256(buf)
	copy(cs[:], c[:])
	return
}

// applyUpdate applies an update to a silo on disk
func (su siloUpdate) applyUpdate(f *os.File, dataPath string) error {
	// Write number
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data[:], su.number)
	_, err := f.WriteAt(data[:], su.offset)
	if err != nil {
		return err
	}

	// Write new checksum
	_, err = f.WriteAt(su.newChecksum[:], su.newChecksumOffset)
	if err != nil {
		return err
	}

	// Delete old data file if it still exists
	err = os.Remove(filepath.Join(dataPath, hex.EncodeToString(su.oldChecksum[:])))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// init calculates the checksum of the silo and writes it to disk
func (s *silo) init() error {
	cs := s.checksum()
	_, err := s.f.WriteAt(cs[:], s.offset+int64(len(s.numbers)*4))
	return err
}

// threadedSetupWrite simulates a setup by updating the checksum and writing
// random data to a file which uses the checksum as a filename
func (s *silo) threadedSetupWrite(done chan error, dataPath string, ncs checksum) {
	// signal completion
	defer close(done)

	// write new data file
	newFile, err := os.Create(filepath.Join(dataPath, hex.EncodeToString(ncs[:])))
	if err != nil {
		done <- err
		return
	}
	_, err = newFile.Write(fastrand.Bytes(10 * pageSize))
	if err != nil {
		done <- err
		return
	}
	syncErr := newFile.Sync()
	if err := newFile.Close(); err != nil {
		done <- err
		return
	}

	// sync changes
	done <- syncErr
}

// threadedUpdate updates a silo until a sync fails
func (s *silo) threadedUpdate(w *writeaheadlog.WAL, dataPath string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Allocate some memory for the updates
	sus := make([]*siloUpdate, 0, len(s.numbers))

	// This thread will execute until the dependency of the silo causes a file
	// sync to fail
	for {
		// Change between 1 and len(s.numbers)
		length := fastrand.Intn(len(s.numbers)) + 1
		ocs := s.checksum()
		appendFrom := length
		var ncs checksum
		for j := 0; j < length; j++ {
			if appendFrom == length && j > 0 && fastrand.Intn(500) == 0 {
				// There is a 0.5% chance that the remaing updates will be
				// appended after the transaction was created
				appendFrom = j
			}
			if s.nextNumber == 0 {
				s.numbers[s.nextNumber] = (s.numbers[len(s.numbers)-1] + 1)
			} else {
				s.numbers[s.nextNumber] = (s.numbers[s.nextNumber-1] + 1)
			}
			ncs = s.checksum()

			// Create siloUpdate
			su := s.newSiloUpdate(s.nextNumber, s.numbers[s.nextNumber], ocs)
			sus = append(sus, su)

			// Increment the index. If that means we reach the end, set it to 0
			s.nextNumber = (s.nextNumber + 1) % uint32(len(s.numbers))
		}

		// Set the siloUpdates checksum and create the corresponding update
		updates := make([]writeaheadlog.Update, 0, len(s.numbers))
		for _, su := range sus {
			copy(su.newChecksum[:], ncs[:])
			updates = append(updates, su.newUpdate())
		}

		// Start setup write
		wait := make(chan error)
		go s.threadedSetupWrite(wait, dataPath, ncs)

		// Create txn
		txn, err := w.NewTransaction(updates[:appendFrom])
		if err != nil {
			panic(err)
		}

		// Append the remaining updates
		if err := <-txn.Append(updates[appendFrom:]); err != nil {
			panic(err)
		}

		// Wait for setup to finish. If it wasn't successful there is no need
		// to continue
		if err := <-wait; err != nil {
			panic(err)
		}

		// Signal setup complete
		if err := <-txn.SignalSetupComplete(); err != nil {
			panic(err)
		}

		// Apply the updates
		for _, su := range sus {
			if err := su.applyUpdate(s.f, dataPath); err != nil {
				panic(err)
			}
		}

		// Sync the updates
		if err := s.f.Sync(); err != nil {
			panic(err)
		}

		// Signal release complete
		if err := txn.SignalUpdatesApplied(); err != nil {
			panic(err)
		}

		// Reset
		sus = sus[:0]
		updates = updates[:0]
	}
}

// recoverSiloWAL recovers the WAL after a crash. This will be called
// repeatedly until it finishes.
func recoverSiloWAL(walPath string, silos map[int64]*silo, testdir string, dbFile *os.File, numSilos int64, numIncrease int) (err error) {
	// Reload wal.
	updates, wal, err := writeaheadlog.New(walPath)
	if err != nil {
		return errors.Extend(errors.New("failed to reload WAL"), err)
	}

	// Unmarshal updates and apply them
	var checksums = make(map[string]struct{})
	for _, update := range updates {
		var su siloUpdate
		su.unmarshal(update.Instructions)
		if err := su.applyUpdate(dbFile, testdir); err != nil {
			return errors.Extend(errors.New("Failed to apply update"), err)
		}
		// Remember new checksums to be able to remove unnecessary setup files
		// later
		checksums[hex.EncodeToString(su.newChecksum[:])] = struct{}{}
	}

	// Sync the applied updates
	if err := dbFile.Sync(); err != nil {
		return errors.Extend(errors.New("Failed to sync database"), err)
	}

	// Remove unnecessary setup files
	files, err := ioutil.ReadDir(testdir)
	if err != nil {
		return errors.Extend(errors.New("Failed to get list of files in testdir"), err)
	}
	for _, f := range files {
		_, exists := checksums[f.Name()]
		if len(f.Name()) == 32 && !exists {
			if err := os.Remove(filepath.Join(testdir, f.Name())); err != nil && !os.IsNotExist(err) {

				return errors.Extend(errors.New("Failed to remove setup file"), err)
			}
		}
	}

	// Check if the checksums match the data
	numbers := make([]byte, numSilos*int64(numIncrease)*4)
	var cs checksum
	for _, silo := range silos {
		// Adjust the size of numbers
		numbers = numbers[:4*len(silo.numbers)]

		// Read numbers and checksum
		if _, err := silo.f.ReadAt(numbers, silo.offset); err != nil {
			return errors.Extend(errors.New("Failed to read numbers of silo"), err)
		}
		if _, err := silo.f.ReadAt(cs[:], silo.offset+int64(4*len(silo.numbers))); err != nil {
			return errors.Extend(errors.New("Failed to read checksum of silo"), err)
		}

		// The checksum should match
		c := blake2b.Sum256(numbers)
		if bytes.Compare(c[:checksumSize], cs[:]) != 0 {
			log.Printf("read cs: %v", cs[:checksumSize])
			log.Printf("calc cs: %v", c[:checksumSize])
			log.Printf("read num: %v", numbers)
			return errors.New("Checksums don't match")
		}
	}

	if err := wal.RecoveryComplete(); err != nil {
		return errors.Extend(errors.New("Failed to signal completed recovery"), err)
	}

	// Close the wal
	if err := wal.Close(); err != nil {
		return errors.Extend(errors.New("Failed to close WAL"), err)
	}
	return nil
}

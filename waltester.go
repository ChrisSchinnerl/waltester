package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
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

	// Create silo objects
	recoveredTxns, silos, wal, file, err := newSiloDatabase(dbPath, walPath, testdir, numSilos, numIncrease)
	if err != nil {
		panic(err)
	}

	// Start silo updates if the wal didn't exist
	if len(recoveredTxns) == 0 {
		for _, silo := range silos {
			wg.Add(1)
			go silo.threadedUpdate(wal, testdir, &wg)
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
	_, err = recoverSiloWAL(walPath, silos, testdir, file, numSilos, numIncrease)
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
)

type (
	// A silo is a simulated group of data that requires using the full
	// featureset of the wal for maximum performance. The silo is a list of
	// numbers followed by a checksum. The numbers form a ring (the first
	// number follows the last number) that has strictly increasing values,
	// except for one place within the ring where the next number is smaller
	// than the previous.
	//
	// The checksum is the checksum of the silo data plus some external data.
	// The external data exists in a file with the same name as checksum. The
	// external data should always be available if the silo exists. There
	// should also be exactly one external file per file and not more if the
	// full database is intact and has been consistently managed.
	silo struct {
		// List of numbers in silo and the index of the next number that will
		// be incremented.
		nextNumber uint32
		numbers    []uint32

		// The checksum of last written data file
		cs checksum

		// Utilities
		f      *os.File // database file holding silo
		offset int64    // offset within database
		skip   bool     // skips the next startup and verification of the silo
	}

	// A siloUpdate contains an update to the silo. There is a number that
	// needs to be written (it's the smallest number in the silo), as well as
	// an update to the checksum because the blob file associated with the silo
	// has changed.
	siloUpdate struct {
		offset int64  // Location in the file where we need to write a number.
		number uint32 // The number we need to write.
		silo   int64  // Offset of the silo within the file.

		// Datafile checksum management.
		prevChecksum   checksum // Need to remove the corresponding file.
		newChecksum    checksum // Need to write the new checksum. File already exists.
		checksumOffset int64    // Location to write the checksum.
	}
)

// newSilo creates a new silo of a certain length at a specific offset in the
// file
func newSilo(offset int64, length int, f *os.File, dataPath string) (*silo, error) {
	if length == 0 {
		panic("numbers shouldn't be empty")
	}

	s := &silo{
		offset:  offset,
		numbers: make([]uint32, length, length),
		f:       f,
	}

	randomData := fastrand.Bytes(10 * pageSize)
	s.cs = computeChecksum(randomData)

	// Write initial checksum
	_, err := s.f.WriteAt(s.cs[:], s.offset+int64(len(s.numbers)*4))
	if err != nil {
		return nil, err
	}

	// Do initial setup write
	done := make(chan error)
	go s.threadedSetupWrite(done, dataPath, randomData, s.cs)

	return s, <-done
}

// marshal marshals a siloUpdate
func (su siloUpdate) marshal() []byte {
	data := make([]byte, 28+2*checksumSize)
	binary.LittleEndian.PutUint64(data[0:8], uint64(su.offset))
	binary.LittleEndian.PutUint32(data[8:12], su.number)
	binary.LittleEndian.PutUint64(data[12:20], uint64(su.silo))
	binary.LittleEndian.PutUint64(data[20:28], uint64(su.checksumOffset))
	copy(data[28:28+checksumSize], su.prevChecksum[:])
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
	su.checksumOffset = int64(binary.LittleEndian.Uint64(data[20:28]))
	copy(su.prevChecksum[:], data[28:28+checksumSize])
	copy(su.newChecksum[:], data[28+checksumSize:])
	return
}

// newUpdate create a WAL update from a siloUpdate
func (su *siloUpdate) newUpdate() writeaheadlog.Update {
	update := writeaheadlog.Update{
		Name:         "This is my update. There are others like it but this one is mine",
		Instructions: su.marshal(),
	}
	return update
}

// newSiloUpdate creates a new Silo update for a number at a specific index
func (s *silo) newSiloUpdate(index uint32, number uint32, pcs checksum) *siloUpdate {
	return &siloUpdate{
		number:         number,
		offset:         s.offset + int64(4*(index)),
		silo:           s.offset,
		prevChecksum:   pcs,
		checksumOffset: s.offset + int64(len(s.numbers)*4),
	}
}

// checksum calculates the silos's current checksum given some data
func computeChecksum(d []byte) (cs checksum) {
	c := blake2b.Sum256(d)
	copy(cs[:], c[:])
	return
}

// applyUpdate applies an update to a silo on disk
func (su siloUpdate) applyUpdate(silo *silo, dataPath string) error {
	if silo == nil {
		panic("silo shouldn't be nil")
	}

	// This update should be skipped this time
	if silo.skip {
		return nil
	}

	// Write number
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data[:], su.number)
	_, err := silo.f.WriteAt(data[:], su.offset)
	if err != nil {
		return err
	}

	// Write new checksum
	_, err = silo.f.WriteAt(su.newChecksum[:], su.checksumOffset)
	if err != nil {
		return err
	}

	// If a new datafile was created we can delete the old one
	if bytes.Compare(su.prevChecksum[:], su.newChecksum[:]) != 0 {
		err = os.Remove(filepath.Join(dataPath, hex.EncodeToString(su.prevChecksum[:])))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// Update the silos cs field
	silo.cs = su.newChecksum
	return nil
}

// threadedSetupWrite simulates a setup by updating the checksum and writing
// random data to a file which uses the checksum as a filename
func (s *silo) threadedSetupWrite(done chan error, dataPath string, randomData []byte, ncs checksum) {
	// signal completion
	defer close(done)

	// write new data file
	newFile, err := os.Create(filepath.Join(dataPath, hex.EncodeToString(ncs[:])))
	if err != nil {
		done <- err
		return
	}
	_, err = newFile.Write(randomData)
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

	// This silo has unfinished transactions. Skip it.
	if s.skip {
		return
	}

	// Allocate some memory for the updates
	sus := make([]*siloUpdate, 0, len(s.numbers))

	// Create some random data and the corresponding checksum for creating data
	// files
	randomData := fastrand.Bytes(10 * pageSize)
	ncs := computeChecksum(randomData)

	// Make sure we start at the beginning of the numbers slice. We need this
	// since there is a chance that nextNumber gets increased without updates
	// being applied. This means that we miss a couple of numbers and we might
	// end up with a slice like [1,2,3,0,0,0,1,2,3,4,5,0,0,0] which is
	// corrupted.
	s.nextNumber = 0

	// This thread will execute until the dependency of the silo causes a file
	// sync to fail
	for {
		// Change between 1 and len(s.numbers)
		length := fastrand.Intn(len(s.numbers)) + 1
		appendFrom := length
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

			// Create siloUpdate
			su := s.newSiloUpdate(s.nextNumber, s.numbers[s.nextNumber], s.cs)
			sus = append(sus, su)

			// Increment the index. If that means we reach the end, set it to 0
			s.nextNumber = (s.nextNumber + 1) % uint32(len(s.numbers))
		}

		// 10% chance to write new data file
		newFile := false
		if fastrand.Intn(10) == 0 {
			newFile = true
		}

		// Set the siloUpdates checksum correctly and create the corresponding
		// update
		updates := make([]writeaheadlog.Update, 0, len(s.numbers))
		for _, su := range sus {
			// If we create a new file we need to set the checksum accordingly
			if newFile {
				copy(su.newChecksum[:], ncs[:])
			} else {
				su.newChecksum = su.prevChecksum
			}
			updates = append(updates, su.newUpdate())
		}

		// Create txn
		txn, err := w.NewTransaction(updates[:appendFrom])
		if err != nil {
			panic(err)
		}

		// Create new file during setup write if necessary
		wait := make(chan error)
		if newFile {
			go s.threadedSetupWrite(wait, dataPath, randomData, ncs)
		} else {
			close(wait)
		}

		// Append the remaining updates
		if err := <-txn.Append(updates[appendFrom:]); err != nil {
			return
		}

		// Wait for setup to finish. If it wasn't successful there is no need
		// to continue
		if err := <-wait; err != nil {
			return
		}

		// Signal setup complete
		if err := <-txn.SignalSetupComplete(); err != nil {
			return
		}

		// Reset random data and checksum if we used it
		if newFile {
			randomData = fastrand.Bytes(10 * pageSize)
			ncs = computeChecksum(randomData)
		}

		// Apply the updates
		for _, su := range sus {
			if err := su.applyUpdate(s, dataPath); err != nil {
				panic(err)
			}
		}

		// Sync the updates
		if err := s.f.Sync(); err != nil {
			return
		}

		// Signal release complete
		if err := txn.SignalUpdatesApplied(); err != nil {
			return
		}

		// Reset
		sus = sus[:0]
		updates = updates[:0]
	}
}

// toUint32Slice is a helper function to convert a byte slice to a uint32
// slice
func toUint32Slice(d []byte) []uint32 {
	buf := bytes.NewBuffer(d)
	converted := make([]uint32, len(d)/4, len(d)/4)
	for i := 0; i < len(converted); i++ {
		converted[i] = binary.LittleEndian.Uint32(buf.Next(4))
	}
	return converted
}

// verifyNumbers checks the numbers of a silo for corruption
func verifyNumbers(numbers []uint32) error {
	// Handle corner cases
	if len(numbers) == 0 {
		return errors.New("len of numbers is 0")
	}
	if len(numbers) == 1 {
		return nil
	}

	// Count the number of dips. Shouldn't be greater than 1
	dips := 0
	for i := 1; i < len(numbers); i++ {
		if numbers[i] < numbers[i-1] {
			dips++
		}
	}
	// Check for the dip from the last number to the first number.
	if numbers[0] < numbers[len(numbers)-1] {
		dips++
	}
	if dips > 1 {
		return fmt.Errorf("numbers are corrupted %v", numbers)
	}
	return nil
}

// recoverSiloWAL recovers the WAL after a crash. This will be called
// repeatedly until it finishes.
func recoverSiloWAL(walPath string, silos map[int64]*silo, testdir string, file *os.File, numSilos int64, numIncrease int) (numSkipped int64, err error) {
	// Reload wal.
	recoveredTxns, wal, err := writeaheadlog.New(walPath)
	if err != nil {
		return 0, errors.Extend(errors.New("failed to reload WAL"), err)
	}

	// Unmarshal updates and apply them
	var appliedTxns []*writeaheadlog.Transaction
	for _, txn := range recoveredTxns {
		// 20% chance to skip transaction
		skipTxn := fastrand.Intn(5) == 0
		for _, update := range txn.Updates {
			var su siloUpdate
			su.unmarshal(update.Instructions)
			silos[su.silo].skip = skipTxn
			if err := su.applyUpdate(silos[su.silo], testdir); err != nil {
				return 0, errors.Extend(errors.New("Failed to apply update"), err)
			}
		}
		if !skipTxn {
			appliedTxns = append(appliedTxns, txn)
		}
	}

	// Sync the applied updates
	if err := file.Sync(); err != nil {
		return 0, errors.Extend(errors.New("Failed to sync database"), err)
	}

	// Check numbers and checksums
	numbers := make([]byte, numSilos*int64(numIncrease)*4)
	var cs checksum
	for _, silo := range silos {
		// We didn't apply the changes for this silo. Skip it this time.
		if silo.skip {
			continue
		}
		// Adjust the size of numbers
		numbers = numbers[:4*len(silo.numbers)]

		// Read numbers and checksum
		if _, err := silo.f.ReadAt(numbers, silo.offset); err != nil {
			return 0, errors.Extend(errors.New("Failed to read numbers of silo"), err)
		}
		if _, err := silo.f.ReadAt(cs[:], silo.offset+int64(4*len(silo.numbers))); err != nil {
			return 0, errors.Extend(errors.New("Failed to read checksum of silo"), err)
		}

		// Check numbers for corruption
		parsedNumbers := toUint32Slice(numbers)
		if err := verifyNumbers(parsedNumbers); err != nil {
			return 0, err
		}

		// Check if a file for the checksum exists
		csHex := hex.EncodeToString(cs[:])
		if _, err := os.Stat(filepath.Join(testdir, csHex)); os.IsNotExist(err) {
			return 0, fmt.Errorf("No file for the following checksum exists: %v", csHex)
		}

		// Reset silo numbers in memory
		silo.numbers = parsedNumbers
	}

	for _, txn := range appliedTxns {
		if err := txn.SignalUpdatesApplied(); err != nil {
			return 0, errors.Extend(errors.New("Failed to signal applied updates"), err)
		}
	}

	// Close the wal
	var openTxns int64
	if openTxns, err = wal.CloseIncomplete(); err != nil {
		return 0, errors.Extend(errors.New("Failed to close WAL"), err)
	}

	// Sanity check open transactions
	if int64(len(recoveredTxns)-len(appliedTxns)) != openTxns {
		panic("Number of skipped txns doesn't match number of open txns")
	}
	return openTxns, nil
}

// newSiloDatabase will create a silo database that overwrites any existing
// silo database.
func newSiloDatabase(dbPath, walPath string, dataPath string, numSilos int64, numIncrease int) ([]*writeaheadlog.Transaction, map[int64]*silo, *writeaheadlog.WAL, *os.File, error) {
	// Create the database file.
	file, err := os.Create(dbPath)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	// Create the wal.
	recoveredTxns, wal, err := writeaheadlog.New(walPath)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Create and initialize the silos.
	var siloOffset int64
	var siloOffsets []int64
	var silos = make(map[int64]*silo)
	for i := 0; int64(i) < numSilos; i++ {
		silo, err := newSilo(siloOffset, 1+i*numIncrease, file, dataPath)
		if err != nil {
			return nil, nil, nil, nil, errors.Extend(errors.New("failed to init silo"), err)
		}
		siloOffsets = append(siloOffsets, siloOffset)
		silos[siloOffset] = silo
		siloOffset += int64(len(silo.numbers)*4) + checksumSize
	}
	return recoveredTxns, silos, wal, file, nil
}

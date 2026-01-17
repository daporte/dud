package index

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

// IoHashTablePath is the default location for the IO hash table
const IoHashTablePath = ".dud/io-hash-table"

// IoHashTable is the type that saves input hash to output hash mapping.
type IoHashTable map[string]string

// ComputeHashFromChecksums returns the SHA256 hash for a sorted slice of checksums.
func ComputeHashFromChecksums(checksums []string) string {
	sort.Strings(checksums)
	h := sha256.New()
	for _, c := range checksums {
		h.Write([]byte(c))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// LoadIoHashTable loads or creates an empty IoHashTable at the given root.
func LoadIoHashTable(rootDir string) (IoHashTable, error) {
	tablePath := filepath.Join(rootDir, IoHashTablePath)
	f, err := os.Open(tablePath)
	if os.IsNotExist(err) {
		return make(IoHashTable), nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var table IoHashTable
	if err := json.NewDecoder(f).Decode(&table); err != nil {
		return nil, err
	}
	return table, nil
}

// SaveIoHashTable writes the IoHashTable into the project root.
func SaveIoHashTable(table IoHashTable, rootDir string) error {
	tablePath := filepath.Join(rootDir, IoHashTablePath)
	f, err := os.Create(tablePath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(table)
}

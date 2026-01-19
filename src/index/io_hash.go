package index

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sort"

	"github.com/kevin-hanselman/dud/src/artifact"
	"github.com/kevin-hanselman/dud/src/checksum"
	"github.com/spf13/viper"
)


type OutputSet map[string]string // output_path -> checksum

type IoHashTable map[string]OutputSet


func CalcStageKey(inputs map[string]*artifact.Artifact, command, workdir string, rootDir string) string {
	var sums []string
	for _, art := range inputs {
		sum := art.Checksum
		if sum == "" && art.Path != "" {
			// Compute checksum from the file's contents if not already set
			filePath := art.Path
			if !strings.HasPrefix(filePath, "/") {
				filePath = filepath.Join(rootDir, filePath)
			}
			if f, err := os.Open(filePath); err == nil {
				if got, err := checksum.Checksum(f); err == nil {
					sum = got
				}
				f.Close()
			}
		}
		fmt.Printf("[DEBUG] input artifact path: %s, checksum: %s\n", art.Path, sum)
		sums = append(sums, sum)
	}
	sort.Strings(sums)
	h := sha256.New()
	for _, sum := range sums {
		h.Write([]byte(sum))
	}
	h.Write([]byte(command))
	h.Write([]byte(workdir))
	return hex.EncodeToString(h.Sum(nil))
}
func getRunCacheDir(rootDir string) string {
	runCacheDir := viper.GetString("run_cache")
	if runCacheDir == "" {
		runCacheDir = ".dud/io-cache"
	}
	if strings.HasPrefix(runCacheDir, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			runCacheDir = filepath.Join(home, runCacheDir[2:])
		}
	}
	return filepath.Join(runCacheDir)
}

func LoadIoHashTable(rootDir string) (IoHashTable, error) {
	runCacheDir := getRunCacheDir(rootDir)
	os.MkdirAll(runCacheDir, 0o755)
	tablePath := filepath.Join(runCacheDir, "io-hash-table")
	// fmt.Printf("[DEBUG] loading hash table from: %s\n", tablePath)
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

func SaveIoHashTable(table IoHashTable, rootDir string) error {
	runCacheDir := getRunCacheDir(rootDir)
	os.MkdirAll(runCacheDir, 0o755)
	tablePath := filepath.Join(runCacheDir, "io-hash-table")
	fmt.Printf("[DEBUG] Saving I/O hash table to: %s\n", tablePath)
	f, err := os.Create(tablePath)
	if err != nil {
		fmt.Printf("[DEBUG] Error creating io-hash-table: %v\n", err)
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(table); err != nil {
		fmt.Printf("[DEBUG] Error encoding io-hash-table: %v\n", err)
		return err
	}
	return nil
}


// IoHashTablePath is the default location for the IO hash table
// const IoHashTablePath = ".dud/io-hash-table"


// ComputeHashFromChecksums returns the SHA256 hash for a sorted slice of checksums.
func ComputeHashFromChecksums(checksums []string) string {
	sort.Strings(checksums)
	h := sha256.New()
	for _, c := range checksums {
		h.Write([]byte(c))
	}
	return hex.EncodeToString(h.Sum(nil))
}

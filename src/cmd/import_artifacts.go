
package cmd

import (
	"io"
	"os"
	"path/filepath"

	"github.com/kevin-hanselman/dud/src/stage"
	"github.com/kevin-hanselman/dud/src/cache"
	"github.com/kevin-hanselman/dud/src/artifact"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var importOutputDir string

var importArtifactsCmd = &cobra.Command{
	Use:   "import-artifacts <stage-file>",
	Short: "Import all outputs from the cache for a given stage file, regardless of index membership.",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		stagePath := args[0]

		rootDir, err := getProjectRootDir()
		if err != nil {
			logger.Error.Fatalf("could not find project root: %v", err)
		}
		if err := readConfig(rootDir); err != nil {
			logger.Error.Fatalf("could not read Dud config: %v", err)
		}
		cacheDir := viper.GetString("cache")
		if cacheDir == "" {
			cacheDir = ".dud/cache"
		}
		ch, err := cache.NewLocalCache(cacheDir)
		if err != nil {
			logger.Error.Fatalf("failed to open Dud cache: %v", err)
		}

		stg, err := stage.FromFile(stagePath)
		if err != nil {
			logger.Error.Fatalf("failed to load stage file: %v", err)
		}

		remote := viper.GetString("remote")

		for _, art := range stg.Outputs {
			if art.Checksum == "" {
				logger.Error.Printf("output %s missing checksum, skipping\n", art.Path)
				continue
			}
			cachePath, err := ch.PathForChecksum(art.Checksum)
			if err != nil {
				logger.Error.Printf("invalid checksum %s for %s: %v", art.Checksum, art.Path, err)
				continue
			}
			cachePath = filepath.Join(cacheDir, cachePath)

			_, statErr := os.Stat(cachePath)
			if os.IsNotExist(statErr) && remote != "" {
				logger.Info.Printf("%s not in cache, trying to fetch from remote\n", art.Path)
				err = ch.Fetch(remote, map[string]*artifact.Artifact{art.Path: art})
				if err != nil {
					logger.Error.Printf("Error fetching %s from remote: %v\n", art.Path, err)
					continue
				}
			}

			_, statErr = os.Stat(cachePath)
			if os.IsNotExist(statErr) {
				logger.Error.Printf("could not locate output %s in cache or remote, skipping\n", art.Path)
				continue
			}
			if art.Checksum == "" {
				logger.Error.Printf("output %s missing checksum, skipping\n", art.Path)
				continue
			}

			// --- OUTPUT DIR LOGIC START ---
			dest := art.Path
			if importOutputDir != "" {
				dest = filepath.Join(importOutputDir, filepath.Base(art.Path))
			}
			// --- OUTPUT DIR LOGIC END ---

			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				logger.Error.Printf("failed to create directory for %s: %v", dest, err)
				continue
			}

			err = os.Link(cachePath, dest)
			if err == nil {
				logger.Info.Printf("Imported (linked) %s from cache to %s\n", art.Path, dest)
				continue
			}
			src, err1 := os.Open(cachePath)
			if err1 != nil {
				logger.Error.Printf("failed to open cache file %s: %v", cachePath, err1)
				continue
			}
			dst, err2 := os.Create(dest)
			if err2 != nil {
				src.Close()
				logger.Error.Printf("failed to create destination file %s: %v", dest, err2)
				continue
			}
			_, err = io.Copy(dst, src)
			src.Close()
			dst.Close()
			if err != nil {
				logger.Error.Printf("failed to copy %s: %v", dest, err)
				continue
			}
			logger.Info.Printf("Imported (copied) %s from cache to %s\n", art.Path, dest)
		}
	},
}

func init() {
	importArtifactsCmd.Flags().StringVarP(
		&importOutputDir,
		"output_dir", "O", "",
		"Directory into which imported files should be placed (overrides default output location)",
	)
	rootCmd.AddCommand(importArtifactsCmd)
}

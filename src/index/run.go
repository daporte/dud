package index

import (
	"os/exec"
	"os"
	"path/filepath"
	"fmt"
	
	"github.com/kevin-hanselman/dud/src/checksum"

	"github.com/kevin-hanselman/dud/src/agglog"
	"github.com/kevin-hanselman/dud/src/cache"
	"github.com/kevin-hanselman/dud/src/strategy"
	"github.com/pkg/errors"
)

// for mocking
var runCommand = func(cmd *exec.Cmd) error {
	return cmd.Run()
}

// Run runs a Stage and all upstream Stages.
func (idx Index) Run(
	stagePath string,
	ch cache.Cache,
	rootDir string,
	recursive bool,
	ran map[string]bool,
	inProgress map[string]bool,
	logger *agglog.AggLogger,
) error {
	if _, ok := ran[stagePath]; ok {
		return nil
	}

    fmt.Printf("Running stage: %s\n", stagePath)
	if inProgress[stagePath] {
		fmt.Printf("cycle detected at stage %s\n", stagePath)
		return errors.New("cycle detected")
	}
	inProgress[stagePath] = true

	stg, ok := idx[stagePath]
	if !ok {
		return unknownStageError{stagePath}
	}

	// At the end of your run function:
	depFile, err := os.Create(filepath.Join(rootDir, ".dud", "stage_deps.txt"))
	if err != nil {
		logger.Error.Printf("failed to create dep file: %v\n", err)
	} else {
		for stagePath, stg := range idx {
			var deps []string
			for inp := range stg.Inputs {
				if owner, _ := idx.findOwner(inp); owner != "" {
					deps = append(deps, owner)
				}
			}
			fmt.Fprintf(depFile, "%s:", stagePath)
			for _, dep := range deps {
				fmt.Fprintf(depFile, " %s", dep)
			}
			fmt.Fprintln(depFile)
		}
		depFile.Close()
	}

	hasCommand := stg.Command != ""
	checksumUpToDate := false

	if stg.Checksum != "" {
		realChecksum, err := stg.CalculateChecksum()
		if err != nil {
			return err
		}
		checksumUpToDate = realChecksum == stg.Checksum
	}

	doRun := false
	var runReason string

	// Run if we have a command and no inputs.
	if hasCommand && (len(stg.Inputs) == 0) {
		doRun = true
		runReason = "has command and no inputs"
	}

	// Run if our checksum is stale.
	if !checksumUpToDate {
		doRun = true
		runReason = "definition modified"
	}
	// Always check all upstream stages.
	for artPath, art := range stg.Inputs {
		ownerPath, _ := idx.findOwner(artPath)
		if ownerPath == "" {
			artStatus, err := ch.Status(rootDir, *art, true)
			if err != nil {
				return err
			}
			if !artStatus.ContentsMatch {
				doRun = true
				runReason = "input out-of-date"
			}
		} else if recursive {
			if err := idx.Run(ownerPath, ch, rootDir, recursive, ran, inProgress, logger); err != nil {
				return err
			}
			if ran[ownerPath] {
				doRun = true
				runReason = "upstream stage out-of-date"
			}
		}
	}
		

	// --- Dud Stage Run Caching ---
	// Before running, check cache for this stage+inputs+command
	stageKey := CalcStageKey(stg.Inputs, stg.Command, stg.WorkingDir)
	table, _ := LoadIoHashTable(rootDir)

	if outSet, ok := table[stageKey]; ok {
		logger.Info.Printf("cache hit: restoring outputs for stage %s from local cache\n", stagePath)

		for path, checksum := range outSet {
			art, exists := stg.Outputs[path]
			if !exists {
				logger.Error.Printf("output %s not found in stage definition", path)
				continue
			}
			art.Checksum = checksum
			if err := ch.Checkout(rootDir, *art, strategy.LinkStrategy, nil); err != nil {
				logger.Error.Printf("failed to check out %s from cache: %v", art.Path, err)
				return err
			}
		}

		ran[stagePath] = true
		delete(inProgress, stagePath)
		return nil
	}


	// --- End Dud Stage Run Caching ---


	if !doRun {
		for _, art := range stg.Outputs {
			artStatus, err := ch.Status(rootDir, *art, true)
			if err != nil {
				return err
			}
			if !artStatus.ContentsMatch {
				doRun = true
				runReason = "output out-of-date"
				break
			}
		}
	}
	if doRun {
		if hasCommand {
			logger.Info.Printf("running stage %s (%s)\n", stagePath, runReason)
			logger.Info.Printf("trying to cache")
			cmd := stg.CreateCommand()
			// Avoid cmd.Command here because it will include "sh -c ...".
			logger.Debug.Printf("(in %s) %s\n", cmd.Dir, stg.Command)
			if err := runCommand(cmd); err != nil {
				return err
			}
			logger.Info.Printf("after runCommand")

			strat := strategy.LinkStrategy
			committed := make(map[string]bool)
			inProgressCommit := make(map[string]bool)
			if err := idx.Commit(stagePath, ch, rootDir, strat, committed, inProgressCommit, logger); err != nil {
				return err
			}
			for _, art := range stg.Inputs {
				if art.Checksum == "" {
					f, err := os.Open(filepath.Join(rootDir, art.Path))
					if err == nil {
						sum, err := checksum.Checksum(f)
						if err == nil {
							art.Checksum = sum
						}
						f.Close()
					}
				}
			}

			// After outputs are committed, save output set to cache table
			outputSet := OutputSet{}
			for path, art := range stg.Outputs {
				outputSet[path] = art.Checksum
			}
			table[stageKey] = outputSet
			if err := SaveIoHashTable(table, rootDir); err != nil {
				logger.Error.Printf("failed to update io-hash-table: %v", err)
			} else {
				logger.Info.Printf("saved new cache state for this stage/configuration\n")
			}


		} else {
			logger.Info.Printf("nothing to do for stage %s (%s, but no command)\n", stagePath, runReason)
		}
	} else {
		logger.Info.Printf("nothing to do for stage %s (up-to-date)\n", stagePath)
	}
	ran[stagePath] = doRun
	delete(inProgress, stagePath)
	return nil
}

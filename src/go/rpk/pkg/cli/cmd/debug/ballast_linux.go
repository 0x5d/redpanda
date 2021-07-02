// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package debug

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"golang.org/x/sys/unix"
)

const defaultBallastPath = "ballast.txt"

func NewBallastCommand() *cobra.Command {
	var (
		path string
		size string
	)
	command := &cobra.Command{
		Use:          "ballast",
		Short:        "Create a \"ballast\" file which can be removed later to free disk space.",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			intSize, err := units.FromHumanSize(size)
			if err != nil {
				return fmt.Errorf("'%s' is not a valid size unit.", size)
			}

			abspath, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf(
					"couldn't resolve the absolute file path for %s: %w",
					path,
					err,
				)
			}

			return executeBallast(abspath, intSize)
		},
	}
	command.Flags().StringVar(
		&path,
		"path",
		filepath.Join(config.Default().Redpanda.Directory, defaultBallastPath),
		"The filepath where the ballast file will be created.",
	)
	command.Flags().StringVar(
		&size,
		"size",
		units.HumanSize(1*units.GiB),
		"The size of the ballast file, e.g. 1GiB, 1024KiB, 3GB.",
	)
	return command
}

func executeBallast(path string, size int64) error {
	_, err := os.Stat(path)
	if err == nil {
		// If the file exists, error out to prevent overwriting important files
		// or filesystems.
		return fmt.Errorf("file '%s' already exists. If you're sure you want"+
			" to replace it, remove it first and then run this command again.",
			path,
		)
	}
	if !os.IsNotExist(err) {
		return err
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := unix.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("couldn't allocate the requested size while"+
			" creating the ballast file at '%s': %w",
			path,
			err,
		)
	}
	return errors.Wrap(f.Sync(), "couldn't sync the ballast file at "+path)
}

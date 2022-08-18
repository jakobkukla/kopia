//go:build darwin || (linux && amd64)
// +build darwin linux,amd64

package robustness

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/robustness/fiofilewriter"
)

func TestManySmallFiles(t *testing.T) {

	//create or connect to repo in format version 1

	const (
		fileSize = 4096
		numFiles = 100
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	// kopiaExe := os.Getenv("KOPIA_EXE")

	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	require.NoError(t, err)

	// //connect, /var/folders/cb/7k5w4dgs6c1gs3m0n1qf2rkc0000gp/T/engine-data-2798515155/kopia-config3684487035/.kopia.config
	// out, err := exec.Command(kopiaExe, "repo", "connect", "filesystem", "--path", "/Users/chaitali.gondhalekar/Work/Kasten/kopia_dummy_repo/robustness-data", "--content-cache-size-mb", "500", "--metadata-cache-size-mb", "500", "--no-check-for-updates", "--password", "qWQPJ2hiiLgWRRCr").Output()
	// if err != nil {
	// 	t.FailNow()
	// }
	// log.Println("repo connect: ", string(out))

	// //repo status
	// out, err = exec.Command(kopiaExe, "repository", "status", "--json").Output()
	// if err != nil {
	// 	t.FailNow()
	// }
	// log.Println("repo status: ", string(out))

	// //snapshot
	// dataDir := ""
	// // /Users/chaitali.gondhalekar/Work/Kasten/kopia_dummy_repo/fio-data-1578447550'
	// out, err = exec.Command(kopiaExe, "snapshot", "--parallel", "8", "--no-progress", dataDir).Output()
	// // out, err := cmd.CombinedOutput()
	// if err != nil {
	// 	t.FailNow()
	// }
	// log.Println("snapshot output: ", string(out))

	// // assign the snap ID in debugger
	// snapID := "k6b45bf2d9c17daa5e8301f11080813b7"
	// // restore using CLI
	// cmd := exec.Command(kopiaExe, "restore", snapID, t.TempDir())

	// out, err = cmd.CombinedOutput()
	// if err != nil {
	// 	t.FailNow()
	// }
	// log.Println(out)
}

func TestOneLargeFile(t *testing.T) {
	const (
		fileSize = 40 * 1024 * 1024
		numFiles = 1
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(1),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	require.NoError(t, err)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	// TODO: Test takes too long - need to address performance issues with fio writes
	const (
		fileSize      = 4096
		numFiles      = 1000
		filesPerWrite = 10
		actionRepeats = numFiles / filesPerWrite
	)

	fileWriteOpts := map[string]string{
		fiofilewriter.MaxDirDepthField:         strconv.Itoa(15),
		fiofilewriter.MaxFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MinFileSizeField:         strconv.Itoa(fileSize),
		fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		fiofilewriter.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:             strconv.Itoa(actionRepeats),
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	_, err := eng.ExecAction(ctx, engine.WriteRandomFilesActionKey, fileWriteOpts)
	require.NoError(t, err)

	snapOut, err := eng.ExecAction(ctx, engine.SnapshotDirActionKey, nil)
	require.NoError(t, err)

	_, err = eng.ExecAction(ctx, engine.RestoreSnapshotActionKey, snapOut)
	require.NoError(t, err)
}

func TestRandomizedSmall(t *testing.T) {
	st := timetrack.StartTimer()

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotDirActionKey):              strconv.Itoa(2),
			string(engine.RestoreSnapshotActionKey):          strconv.Itoa(2),
			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(8),
			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
		},
		engine.WriteRandomFilesActionKey: map[string]string{
			fiofilewriter.IOLimitPerWriteAction:    fmt.Sprintf("%d", 512*1024*1024),
			fiofilewriter.MaxNumFilesPerWriteField: strconv.Itoa(100),
			fiofilewriter.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			fiofilewriter.MaxDirDepthField:         strconv.Itoa(3),
		},
	}

	ctx := testlogging.ContextWithLevel(t, testlogging.LevelInfo)

	for st.Elapsed() <= *randomizedTestDur {
		err := eng.RandomAction(ctx, opts)
		if errors.Is(err, robustness.ErrNoOp) {
			t.Log("Random action resulted in no-op")

			err = nil
		}

		require.NoError(t, err)
	}
}

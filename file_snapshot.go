package papillon

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

const (
	dirMode       = 0755
	testFile      = "snapShotTest"
	tmpFileSuffix = ".tmp"
	metaFile      = "meta.json"
	snapshotFile  = "snapshot.bin"
)

type (
	FileWithSync interface {
		fs.File
		Sync() error
	}
	FileSnapshot struct {
		dir         string
		noSync      bool
		retainCount int
	}

	FileSnapshotSink struct {
		snapshotStore *FileSnapshot
		close         bool
		meta          *fileSnapshotMeta
		dir           string
		parentPath    string
		noSync        bool
		hash          hash.Hash64
		file          FileWithSync
		writer        *bufio.Writer
	}
	fileSnapshotMeta struct {
		*SnapShotMeta
		CRC []byte
	}
	bufferReader struct {
		buf  *bufio.Reader
		file FileWithSync
	}
)

func newCRC64() hash.Hash64 {
	return crc64.New(crc64.MakeTable(crc64.ECMA))
}
func sortMetaList(list []*fileSnapshotMeta) {
	sort.Slice(list, func(i, j int) bool {
		if list[i].Term != list[j].Term {
			return list[i].Term > list[j].Term
		}
		if list[i].Index != list[j].Index {
			return list[i].Index > list[j].Index
		}
		return list[i].ID > list[j].ID
	})
}

func (b *bufferReader) Read(p []byte) (n int, err error) {
	return b.buf.Read(p)
}

func (b *bufferReader) Close() error {
	return b.file.Close()
}

func NewFileSnapshot(dirPath string, noSync bool, retainCount int) (*FileSnapshot, error) {
	if err := os.MkdirAll(dirPath, dirMode); err != nil && !os.IsExist(err) {
		return nil, err
	}

	snapshot := &FileSnapshot{
		dir:         dirPath,
		noSync:      noSync,
		retainCount: retainCount,
	}
	if err := snapshot.testCreatePermission(); err != nil {
		return nil, fmt.Errorf("test create permissions failed :%s", err)
	}
	return snapshot, nil
}

func (f *FileSnapshot) testCreatePermission() error {
	filePath := filepath.Join(f.dir, testFile)
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	_ = file.Close()
	return os.Remove(filePath)
}

func (f *FileSnapshot) readMeta(id string) (meta *fileSnapshotMeta, err error) {
	metaFilePath := filepath.Join(f.dir, id, metaFile)
	file, err := os.Open(metaFilePath)
	if err != nil {
		return nil, err
	}
	err = json.NewDecoder(file).Decode(&meta)
	return
}

func (f *FileSnapshot) Open(id string) (*SnapShotMeta, io.ReadCloser, error) {
	meta, err := f.readMeta(id)
	if err != nil {
		return nil, nil, err
	}

	snapshotFilePath := filepath.Join(f.dir, id, snapshotFile)
	file, err := os.Open(snapshotFilePath)
	defer func() {
		if err != nil {
			file.Close()
		}
	}()
	if err != nil {
		return nil, nil, err
	}
	hash64 := newCRC64()
	if _, err = io.Copy(hash64, file); err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(hash64.Sum(nil), meta.CRC) {
		return nil, nil, errors.New("CRC mismatch")
	}
	if _, err = file.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	return meta.SnapShotMeta, &bufferReader{
		buf:  bufio.NewReader(file),
		file: file,
	}, nil
}

func (f *FileSnapshot) List() (list []*SnapShotMeta, err error) {
	metaList, err := f.getSnapshots()
	if err != nil {
		return nil, err
	}
	for _, meta := range metaList {
		list = append(list, meta.SnapShotMeta)
		if len(list) == f.retainCount {
			break
		}
	}
	return
}

func (f *FileSnapshot) getSnapshots() (metaList []*fileSnapshotMeta, err error) {
	dirList, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range dirList {
		if !entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), tmpFileSuffix) {
			continue
		}
		meta, err := f.readMeta(entry.Name())
		if err != nil {
			return nil, err
		}
		metaList = append(metaList, meta)
	}
	sortMetaList(metaList)
	return
}
func (s *FileSnapshotSink) writeMeta() error {
	fileName := filepath.Join(s.dir, metaFile)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	enc := json.NewEncoder(file)
	enc.SetIndent("", "	")
	if err = enc.Encode(s.meta); err != nil {
		return err
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	if !s.noSync {
		if err = file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileSnapshot) Create(version SnapShotVersion, index, term uint64, configuration Configuration, configurationIndex uint64, rpc RpcInterface) (SnapshotSink, error) {
	name := snapshotName(term, index)
	snapshotDir := filepath.Join(f.dir, name+tmpFileSuffix)
	if err := os.MkdirAll(snapshotDir, dirMode); err != nil {
		return nil, err
	}
	sink := &FileSnapshotSink{
		snapshotStore: f,
		dir:           snapshotDir,
		noSync:        f.noSync,
		parentPath:    f.dir,
		meta: &fileSnapshotMeta{
			SnapShotMeta: &SnapShotMeta{
				Version:            version,
				ID:                 name,
				Index:              index,
				Term:               term,
				Configuration:      configuration,
				ConfigurationIndex: configurationIndex,
			},
		},
		hash: newCRC64(),
	}
	if err := sink.writeMeta(); err != nil {
		return nil, err
	}
	snapshotPath := filepath.Join(snapshotDir, snapshotFile)
	if file, err := os.Create(snapshotPath); err != nil {
		return nil, err
	} else {
		sink.file = file
		sink.writer = bufio.NewWriter(io.MultiWriter(sink.hash, file))
	}
	return sink, nil
}

func (f *FileSnapshotSink) Write(p []byte) (n int, err error) {
	return f.writer.Write(p)
}

func (f *FileSnapshotSink) Close() error {
	if f.close {
		return nil
	}
	f.close = true
	if err := f.finalize(); err != nil {
		return err
	}
	if err := f.writeMeta(); err != nil {
		return err
	}
	newPath := strings.TrimSuffix(f.dir, tmpFileSuffix)

	if err := os.Rename(f.dir, newPath); err != nil {
		return err
	}
	if err := func() error {
		if !(!f.noSync && runtime.GOOS != "windows") {
			return nil
		}
		// 对于目录也需要执行 fsync 操作 https://man7.org/linux/man-pages/man2/fsync.2.html
		file, err := os.Open(f.parentPath)
		if err != nil {
			return err
		}
		defer file.Close()
		return file.Sync()
	}(); err != nil {
		return err
	}

	return f.snapshotStore.reapSnapshot()

}

func (f *FileSnapshotSink) ID() string {
	return f.meta.ID
}

func (f *FileSnapshotSink) Cancel() error {
	if err := f.finalize(); err != nil {
		return err
	}
	return os.RemoveAll(f.dir)
}

func (f *FileSnapshotSink) finalize() error {
	if err := f.writer.Flush(); err != nil {
		return err
	}
	if !f.noSync {
		if err := f.file.Sync(); err != nil {
			return err
		}
	}
	fileInfo, err := f.file.Stat()
	if err != nil {
		return err
	}
	if err = f.file.Close(); err != nil {
		return err
	}
	f.meta.Size = fileInfo.Size()
	f.meta.CRC = f.hash.Sum(nil)
	return nil
}

func (f *FileSnapshot) reapSnapshot() error {
	metaList, err := f.getSnapshots()
	if err != nil {
		return err
	}
	if len(metaList) < f.retainCount {
		return nil
	}
	for _, meta := range metaList[f.retainCount:] {
		filePath := filepath.Join(f.dir, meta.ID)
		if err := os.RemoveAll(filePath); err != nil {
			return err
		}
	}
	return nil
}

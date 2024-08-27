package godis

import (
	"os"
	"path/filepath"
	"sync"
)

// 抽象一层IO接口 来匹配不同的文件系统
type ioManager interface {
	Name() string
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Sync() error
	Close() error
}

const FileName = "godis.data"
const MergeFileName = "godis.data.merge"

// DBFile 数据文件定义
type dbFile struct {
	File          ioManager
	Offset        int64
	HeaderBufPool *sync.Pool
}

//数据缓冲区
type buffer struct{
	bytes []byte
}


func newInternal(fileName string) (*dbFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	pool := &sync.Pool{New: func() interface{} {
		//argument should be pointer-like to avoid allocations (SA6002)
		//return make([]byte, entryHeaderSize)
		buf := new(buffer)
		buf.bytes = make([]byte, entryHeaderSize)
		return buf
	}}
	return &dbFile{Offset: stat.Size(), File: file, HeaderBufPool: pool}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string) (*dbFile, error) {
	fileName := filepath.Join(path, FileName)
	return newInternal(fileName)
}

// NewMergeDBFile 新建一个合并时的数据文件
func NewMergeDBFile(path string) (*dbFile, error) {
	fileName := filepath.Join(path, MergeFileName)
	return newInternal(fileName)
}

// Read 从 offset 处开始读取
func (df *dbFile) Read(offset int64) (e *Entry, err error) {
	buf := df.HeaderBufPool.Get().(*buffer)
	defer df.HeaderBufPool.Put(buf)
	if _, err = df.File.ReadAt(buf.bytes, offset); err != nil {
		return
	}
	if e, err = Decode(buf.bytes); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// Write 写入 Entry
func (df *dbFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}

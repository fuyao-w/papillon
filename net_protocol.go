package papillon

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

/*
协议：${魔数 1byte} ${请求类型 1 byte}  ${请求包体长度 8 byte [BigEndian]} ${请求包体}
*/
const (
	magic byte = 3
)

var (
	errUnrecognizedRequest = errors.New("unrecognized request")
)

type DefaultPackageParser struct{}

var defaultPackageParser = new(DefaultPackageParser)

func (d *DefaultPackageParser) Encode(writer *bufio.Writer, cmdType rpcType, data []byte) (err error) {
	for _, f := range []func() error{
		func() error { return writer.WriteByte(magic) },                                   // magic
		func() error { return writer.WriteByte(byte(cmdType)) },                           // 命令类型
		func() error { return binary.Write(writer, binary.BigEndian, uint64(len(data))) }, // 包体长度
		func() error { _, e := writer.Write(data); return e },                             // 包体
	} {
		if err = f(); err != nil {
			return
		}
	}
	return err
}

func (d *DefaultPackageParser) Decode(reader *bufio.Reader) (rpcType, []byte, error) {
	_magic, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	if _magic != magic {
		return 0, nil, errUnrecognizedRequest
	}

	// 获取命令类型
	ct, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	// 获取包体长度
	var pkgLength uint64
	if err = binary.Read(reader, binary.BigEndian, &pkgLength); err != nil {
		return 0, nil, err
	}

	// 获取包体
	req := make([]byte, pkgLength)
	_, err = io.ReadFull(reader, req)
	return rpcType(ct), req, err
}

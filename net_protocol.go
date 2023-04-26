package papillon

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

/*
协议：${魔数 1byte 0x3} ${ 请求类型 1 byte}  ${包体长度 不固定} \n 包体
*/
const (
	delim = '\n'
	magic = 0x3
)

type DefaultPackageParser struct{}

var defaultPackageParser = new(DefaultPackageParser)

func (d *DefaultPackageParser) Encode(writer *bufio.Writer, cmdType rpcType, data []byte) (err error) {
	for _, f := range []func() error{
		func() error { return writer.WriteByte(magic) },                                // magic
		func() error { return writer.WriteByte(byte(cmdType)) },                        // 命令类型
		func() error { _, e := writer.WriteString(strconv.Itoa(len(data))); return e }, // 包体长度
		func() error { return writer.WriteByte(delim) },                                // 分割符
		func() error { _, e := writer.Write(data); return e },                          // 包体
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
		return 0, nil, errors.New("unrecognized request")
	}

	// 获取命令类型
	ct, err := reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}

	// 获取包体长度
	pkgLength, err := reader.ReadString(delim)
	if err != nil {
		return 0, nil, err
	}

	// 获取包体
	length, err := strconv.Atoi(strings.TrimRight(pkgLength, string(delim)))
	if err != nil {
		return 0, nil, err
	}

	buf := make([]byte, length)
	_, err = reader.Read(buf)
	return rpcType(ct), buf, err
}

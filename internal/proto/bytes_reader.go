package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"github.com/redis/go-redis/v9/internal/util"
)

type BytesReader struct {
	Reader *Reader
}

func NewBytesReader(r *Reader) *BytesReader {
	return &BytesReader{
		Reader: r,
	}
}

func (r *BytesReader) ReadReply(writer *bytes.Buffer) error {
	line, err := r.readLine()
	if err != nil {
		return err
	}

	switch line[0] {
	case RespError:
		return RedisError(line[1 : len(line)-2])
	case RespBlobError:
		blobErr, err := r.Reader.readStringReply(line)
		if err != nil {
			if err == Nil {
				return nil
			}
			return err
		}
		return RedisError(blobErr)
	default:
		if _, err := writer.Write(line); err != nil {
			return err
		}
		switch line[0] {
		case RespNil, RespStatus, RespInt, RespFloat, RespBool, RespBigInt:
			return nil
		case RespString, RespVerbatim:
			return r.readStringReply(line, writer)
		case RespArray, RespSet, RespPush:
			return r.readSlice(line, writer)
		case RespMap:
			return r.readMap(line, writer)
		case RespAttr:
			// part attr
			if err := r.ReadReply(writer); err != nil {
				return err
			}
			return r.ReadReply(writer)
		}
	}

	// Compatible with RESP2
	if IsNilReply(line) {
		return nil
	}

	return fmt.Errorf("redis: can't parse %.100q", line)
}

func (r *BytesReader) readLine() ([]byte, error) {
	b, err := r.Reader.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.Reader.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	return b, nil
}

func (r *BytesReader) readStringReply(line []byte, writer *bytes.Buffer) error {
	n, err := rawReplyLen(line)
	if err != nil {
		return err
	}
	if n == -1 {
		return nil
	}

	_, err = writer.ReadFrom(io.LimitReader(r.Reader.rd, int64(n+2)))
	return err
}

func (r *BytesReader) readSlice(line []byte, writer *bytes.Buffer) error {
	n, err := rawReplyLen(line)
	if err != nil {
		return err
	}
	if n <= 0 {
		return nil
	}

	for i := 0; i < n; i++ {
		if err := r.ReadReply(writer); err != nil {
			if _, ok := err.(RedisError); ok {
				continue
			}
			return err
		}
	}
	return nil
}

func (r *BytesReader) readMap(line []byte, writer *bytes.Buffer) error {
	n, err := rawReplyLen(line)
	if err != nil {
		return err
	}
	if n <= 0 {
		return nil
	}

	for i := 0; i < n; i++ {
		// read key
		if err := r.ReadReply(writer); err != nil {
			return err
		}
		// read value
		if err := r.ReadReply(writer); err != nil {
			if _, ok := err.(RedisError); ok {
				continue
			}
			return err
		}
	}
	return nil
}

func rawReplyLen(line []byte) (n int, err error) {
	n, err = util.Atoi(line[1 : len(line)-2])
	if err != nil {
		return 0, err
	}

	if n < -1 {
		return 0, fmt.Errorf("redis: invalid reply: %q", line)
	}

	return n, nil
}

package proto

import (
	"fmt"
	"io"
)

type BytesReader struct {
	Reader *Reader
}

func NewBytesReader(r *Reader) *BytesReader {
	return &BytesReader{
		Reader: r,
	}
}

func (r *BytesReader) ReadReply(buf io.Writer) error {
	line, err := r.Reader.readLine()
	if err != nil {
		return err
	}

	_, _ = buf.Write(line)
	buf.Write([]byte{'\r', '\n'})
	switch line[0] {
	case RespError, RespNil, RespStatus, RespInt, RespFloat, RespBool, RespBigInt:
		return nil
	case RespBlobError, RespString:
		return r.readStringReply(line, buf)
	case RespVerbatim:
		return r.readVerb(line, buf)
	case RespArray, RespSet, RespPush:
		return r.readSlice(line, buf)
	case RespMap:
		return r.readMap(line, buf)
	case RespAttr:
		// part attr
		if err := r.ReadReply(buf); err != nil {
			return err
		}
		return r.ReadReply(buf)
	}

	// Compatible with RESP2
	if IsNilReply(line) {
		return nil
	}

	return fmt.Errorf("redis: can't parse %.100q", line)
}

func (r *BytesReader) readStringReply(line []byte, buf io.Writer) error {
	n, err := replyLen(line)
	if err != nil {
		return err
	}

	b := make([]byte, n+2)
	_, err = io.ReadFull(r.Reader.rd, b)
	if err != nil {
		return err
	}

	_, _ = buf.Write(b)
	return nil
}

func (r *BytesReader) readVerb(line []byte, buf io.Writer) error {
	n, err := replyLen(line)
	if err != nil {
		return err
	}
	b := make([]byte, n+2)
	_, err = io.ReadFull(r.Reader.rd, b)
	if err != nil {
		return err
	}

	// the first three bytes provide information about the format of the following string,
	// which can be txt for plain text, or mkd for markdown. The fourth byte is always :.
	// the suffix always be /r/n
	if len(b) < 6 || b[3] != ':' {
		return fmt.Errorf("redis: can't parse verbatim string reply: %q", line)
	}

	_, _ = buf.Write(b)
	return nil
}

func (r *BytesReader) readSlice(line []byte, buf io.Writer) error {
	n, err := replyLen(line)
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		if err := r.ReadReply(buf); err != nil {
			return err
		}
	}
	return nil
}

func (r *BytesReader) readMap(line []byte, buf io.Writer) error {
	n, err := replyLen(line)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		// read key
		if err := r.ReadReply(buf); err != nil {
			return err
		}
		// read value
		if err := r.ReadReply(buf); err != nil {
			return err
		}
	}
	return nil
}

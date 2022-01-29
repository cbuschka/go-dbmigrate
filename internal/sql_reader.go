package internal

import (
	"bufio"
	"io"
	"strings"
)

type SqlReader struct {
	bufRd *bufio.Reader
}

func NewSqlReader(rd io.Reader) *SqlReader {
	return &SqlReader{bufio.NewReader(rd)}
}

func (r *SqlReader) ReadStatements() ([]string, error) {

	statements := make([]string, 0)
	for {
		st, err := r.readStatement()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		statements = append(statements, st)
	}

	return statements, nil
}

func (r *SqlReader) readStatement() (string, error) {

	bytesBuf := make([]byte, 0)
	STATE_IN_STATEMENT := 0
	STATE_IN_STRING := 1
	STATE_IN_IDENTIFIER := 2

	state := STATE_IN_STATEMENT
	for {
		b, err := r.bufRd.ReadByte()

		switch state {
		case STATE_IN_STATEMENT:
			if err == io.EOF {
				if len(bytesBuf) == 0 {
					return "", err
				}
				return strings.TrimSpace(string(bytesBuf)), nil
			} else if err != nil {
				return "", err
			}

			switch b {
			case ';':
				return strings.TrimSpace(string(bytesBuf)), nil
			case '\'':
				bytesBuf = append(bytesBuf, b)
				state = STATE_IN_STRING
			case '"':
				bytesBuf = append(bytesBuf, b)
				state = STATE_IN_IDENTIFIER
			default:
				bytesBuf = append(bytesBuf, b)
			}
		case STATE_IN_STRING:
			if err != nil {
				return "", err
			}

			switch b {
			case '\'':
				bytesBuf = append(bytesBuf, b)
				state = STATE_IN_STATEMENT
			default:
				bytesBuf = append(bytesBuf, b)
			}
		case STATE_IN_IDENTIFIER:
			if err != nil {
				return "", err
			}

			switch b {
			case '"':
				bytesBuf = append(bytesBuf, b)
				state = STATE_IN_STATEMENT
			default:
				bytesBuf = append(bytesBuf, b)
			}
		}
	}

}

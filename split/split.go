package split

import "io"

type Split interface {
	Split(reader io.Reader) []string
}

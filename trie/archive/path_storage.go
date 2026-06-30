package archive

import (
	"bytes"
	stdbinary "encoding/binary"
)

var (
	pathNodePrefix = []byte{'B', 'P', 'N', '1'}
	pathTopPrefix  = []byte{'B', 'P', 'T', '1'}
)

type persistedNodePath struct {
	path []byte
	bits int
}

func pathNodeKey(shardID int, path []byte, bits int) []byte {
	pathLen := (bits + 7) / 8
	key := make([]byte, len(pathNodePrefix)+4+2+pathLen)
	copy(key, pathNodePrefix)
	stdbinary.BigEndian.PutUint32(key[len(pathNodePrefix):], uint32(shardID))
	stdbinary.BigEndian.PutUint16(key[len(pathNodePrefix)+4:], uint16(bits))
	copy(key[len(pathNodePrefix)+6:], path[:pathLen])
	if bits%8 != 0 && pathLen > 0 {
		key[len(key)-1] &= byte(0xff << (8 - bits%8))
	}
	return key
}

func pathTopNodeKey(level, prefix int) []byte {
	key := make([]byte, len(pathTopPrefix)+1+4)
	copy(key, pathTopPrefix)
	key[len(pathTopPrefix)] = byte(level)
	stdbinary.BigEndian.PutUint32(key[len(pathTopPrefix)+1:], uint32(prefix))
	return key
}

func IsPathStorageKey(key []byte) bool {
	return bytes.HasPrefix(key, pathNodePrefix) || bytes.HasPrefix(key, pathTopPrefix)
}

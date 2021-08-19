package filestore

import (
	"io"
	"os"
	"unicode/utf8"

	"github.com/golang/snappy"
)

// ensureDirectory creates a directory at path if possible,
// returns an error otherwise.
func ensureDirectory(path string, perm os.FileMode) error {
	src, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, perm); err != nil {
			return err
		}
		return nil
	}
	if src.Mode().IsRegular() {
		return ErrDirectoryIsFile
	}
	return nil
}

// asDirectoryPath returns a directory path that is guaranteed to end in an
// OS-relative directory separator.
func asDirectoryPath(path string) string {
	if path == "" {
		return ""
	}
	r, n := utf8.DecodeLastRuneInString(path)
	if r != utf8.RuneError && n > 0 && r != os.PathSeparator {
		return path + string(os.PathSeparator)
	}
	return path
}

// copyFile copies file src to dst. If dst already exists, it is truncated and overwritten.
// If useCompression is true, then the file data is compressed using Zlib.
func copyFile(src, dst string, useCompression, restore bool) error {
	fin, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fin.Close()

	fout, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer fout.Close()

	if useCompression {
		if restore {
			// restoring means that we have to decompress
			csrc := snappy.NewReader(fin)
			_, err = io.Copy(fout, csrc)
			if err != nil {
				return err
			}
			return nil
		}
		// not restoring, so compress the src to dst
		cdst := snappy.NewWriter(fout)
		defer cdst.Close()
		_, err = io.Copy(cdst, fin)
		if err != nil {
			return err
		}
		return nil
	}
	// no compression, just copy from src to dst
	_, err = io.Copy(fout, fin)
	return err
}

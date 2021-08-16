package filestore

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rasteric/flags"
	"golang.org/x/crypto/blake2b"
)

var ErrDirectoryIsFile = errors.New("directory cannot be created because it is a file")

const Compress = flags.Flag0 // if option is set, then files are compressed with Snappy

// Filestore stores different versions of a file on the local hard disk and
// allows you to retrieve them by path or global FileID.
type Filestore struct {
	Dir     string     // the root directory under which versions are stored
	Options flags.Bits // flag options for configuring the filestore
	// following are various unexported internal properties
	db    *sql.DB       // database connection
	mutex *sync.RWMutex // for synchronization
}

func (fs Filestore) init() error {
	if err := ensureDirectory(fs.Root(), 0700); err != nil {
		return fmt.Errorf("filestore could not create root directory: %w", err)
	}
	fs.mutex = &sync.RWMutex{}
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	// now init the db
	var err error
	fs.db, err = sql.Open("sqlite3", fs.dbPath())
	if err != nil {
		return fmt.Errorf("filestore could not open the database: %w", err)
	}
	_, err = fs.db.Exec("create table if not exists Files (file_id integer primary key, checksum text not null);")
	if err != nil {
		return fs.dbError(err)
	}
	_, err = fs.db.Exec("create unique index if not exists Files_Index on Files(checksum);")
	if err != nil {
		return fs.dbError(err)
	}
	_, err = fs.db.Exec("create table if not exists Versions (version_id integer primary key, path text not null, info text not null, version text not null, date text not null, file integer, foreign key(file) references Files(file_id));")
	if err != nil {
		return fs.dbError(err)
	}
	return nil
}

func (fs Filestore) dbError(err error) error {
	return fmt.Errorf("filestore DB error: %w", err)
}

func (fs Filestore) dbPath() string {
	return fs.Root() + "db.sqlite3"
}

// Root returns the root directory, ending in a directory separator unless it is an
// empty relative directory (== the current directory).
func (fs Filestore) Root() string {
	if fs.Dir == "" {
		return "versions/"
	}
	return asDirectoryPath(fs.Dir)
}

// Add adds a file with given path or updates the existing entries for the file.
// The file is versioned  and a version stored with the given info, tag strings and
// semantic version.
func (fs Filestore) Add(path, info, version string) error {
	if fs.db == nil {
		err := fs.init()
		if err != nil {
			return fs.dbError(err)
		}
	}
	check, err := fs.Checksum(path)
	if err != nil {
		return fmt.Errorf("filestore checksum failed for %s: %w", path, err)
	}
	return fs.addVersion(path, info, version, check)
}

func (fs Filestore) addVersion(path, info, version, check string) error {
	name := filepath.Base(path)
	slashPath := filepath.ToSlash(path)
	rows, err := fs.db.Query("select file_id from Files where checksum=?", check)
	if err != nil {
		return fs.dbError(err)
	}
	var fileID int64
	fileID = 0
	if rows.Next() {
		rows.Scan(&fileID)
	}
	if fileID == 0 {
		// copy the file
		dst := fs.filePath(name, check)
		if err := ensureDirectory(filepath.Dir(dst), 0700); err != nil {
			return fmt.Errorf("filestore unable to create directory %s: %w", dst, err)
		}
		if flags.Has(fs.Options, Compress) {
			dst += ".snappy"
		}
		err := copyFile(path, dst, flags.Has(fs.Options, Compress))
		if err != nil {
			os.Remove(dst)
			return fmt.Errorf("filestore failed to copy file \"%s\" to %s: %w", name, dst, err)
		}
		result, err := fs.db.Exec("insert into Files(checksum) Values(?);", check)
		if err != nil {
			return fs.dbError(err)
		}
		fileID, err = result.LastInsertId()
		if err != nil {
			return fs.dbError(err)
		}
	}
	_, err = fs.db.Exec("insert into Versions(path, info, version, date, file) values(?, ?, ?, datetime('now'), ?);",
		slashPath, info, version, fileID)
	return err
}

// filePath returns a local path in the root directory of the form
// root/checksum/name but with platform-specific separators.
func (fs Filestore) filePath(name, checksum string) string {
	return fs.Root() + checksum + string(os.PathSeparator) + name
}

// Checksum computes a 512 byte Blake2b checksum of a given file.
func (fs Filestore) Checksum(path string) (string, error) {
	hasher, err := blake2b.New512(nil)
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(hasher, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)[:]), nil
}

// Has returns true if versions of the file given by the filepath exist,
// false otherwise.
func (fs Filestore) Has(file string) bool {

	return false
}

// FileVersion represents a particular version of a file.
type FileVersion struct {
	Name        string    // the name of the file, including suffix
	SourcePath  string    // the path of the source file (os path)
	VersionPath string    // the path to the file content on disk in the local filestore (os path)
	Info        string    // the info string
	Version     string    // the version string
	From        time.Time // the datetime on which this version was added
	Checksum    string    // the hex-encoded Blake2b checksum of the file contents of this version
}

// Get returns the latest version of a file at path, or an error if the file
// is not in the filestore.
func (fs Filestore) Get(path string) (FileVersion, error) {

	return FileVersion{}, nil
}

// Restore restores the given file version to destination directory dst.
func (fs Filestore) Restore(version FileVersion, dst string) error {

	return nil
}

// Versions returns FileVersion entries for all versions of a file. Nil is returned if there are no versions.
func (fs Filestore) Versions(file string, limit int) []FileVersion {

	return nil
}

// VersionsAfter returns FileVersion entries for all versions of a file after the given date. Nil
// is returned if there are no versions.
func (fs Filestore) VersionsAfter(file string, after time.Time, limit int) []FileVersion {

	return nil
}

// SimpleSearch returns FileVersion entries for all file info strings starting with terms, combined
// with OR but sorted from more to less matching entries.
func (fs Filestore) SimpleSearch(words []string, limit int) []FileVersion {

	return nil
}

// Search performs an FTS5 term search on the database directly. This requires some knowledge of the database
// organization and FTS5 queries.
func (fs Filestore) Search(term string, limit int) []FileVersion {

	return nil
}

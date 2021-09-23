package filestore

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/metaphone3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rasteric/flags"
	"golang.org/x/crypto/blake2b"
)

var ErrDirectoryIsFile = errors.New("directory cannot be created because it is a file")
var ErrNotOpen = errors.New("filestore is not open")
var ErrInvalidDate = errors.New("filestore entry contains invalid date")

const Compress = flags.Flag0 // if option is set, then files are compressed with Snappy

// Filestore stores different versions of a file on the local hard disk and
// allows you to retrieve them by path or global FileID.
type Filestore struct {
	Dir     string     // the root directory under which versions are stored
	Options flags.Bits // flag options for configuring the filestore
	// following are various unexported internal properties
	db                   *sql.DB       // database connection
	mutex                *sync.RWMutex // for synchronization
	queryIDStmt          *sql.Stmt     // used for querying
	insertFileStmt       *sql.Stmt     // for adding files
	insertVersionStmt    *sql.Stmt     // for adding files
	hasVersionStmt       *sql.Stmt     // for checking a version exists with path as key
	getVersionStmt       *sql.Stmt     // for obtaining the latest version (in terms of date)
	getVersionsStmt      *sql.Stmt     // for obtaining all versions up to a limit
	getVersionsAfterStmt *sql.Stmt     // for obtaining all versions after date with a limit
}

// NewFilestore returns a new filestore based on the given root directory and options.
func NewFilestore(root string, options flags.Bits) *Filestore {
	return &Filestore{Dir: root, Options: options}
}

// Open opens the filestore and prepares it for access.
func (fs *Filestore) Open() error {
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
	_, err = fs.db.Exec("create table if not exists Versions (version_id integer primary key, path text not null, info text not null, fuzzy text not null, version text not null, date text not null, file integer, foreign key(file) references Files(file_id));")
	if err != nil {
		return fs.dbError(err)
	}
	_, err = fs.db.Exec("create virtual table if not exists VersionsFts using FTS5 (content='Versions',prefix='2 3 4',version_id,path,info,fuzzy,version,date,file);")

	fs.queryIDStmt, err = fs.db.Prepare("select file_id from Files where checksum=?;")
	if err != nil {
		return fs.dbError(err)
	}
	fs.insertFileStmt, err = fs.db.Prepare("insert into Files(checksum) Values(?);")
	if err != nil {
		return fs.dbError(err)
	}
	fs.insertVersionStmt, err = fs.db.Prepare("insert into Versions(path, info, fuzzy, version, date, file) values(?, ?, ?, ?, datetime('now'), ?);")
	if err != nil {
		return fs.dbError(err)
	}
	fs.hasVersionStmt, err = fs.db.Prepare("select exists (select 1 from Versions where path=? limit 1);")
	if err != nil {
		return fs.dbError(err)
	}
	fs.getVersionStmt, err = fs.db.Prepare("select version_id, path, info, fuzzy, version, date, checksum from Versions inner join Files on Versions.file=Files.file_id where Versions.path=? order by Versions.date desc limit 1;")
	if err != nil {
		return fs.dbError(err)
	}
	fs.getVersionsStmt, err = fs.db.Prepare("select version_id, path, info, fuzzy, version, date, checksum from Versions inner join Files on Versions.file=Files.file_id where Versions.path=? order by Versions.date desc limit ?;")
	if err != nil {
		return fs.dbError(err)
	}
	fs.getVersionsAfterStmt, err = fs.db.Prepare("select version_id, path, info, fuzzy, version, date, checksum from Versions inner join Files on Versions.file=Files.file_id where Versions.path=? and Versions.date > ? order by Versions.date desc limit ?;")
	if err != nil {
		return fs.dbError(err)
	}
	return nil
}

// Close closes the filestore and frees associated resources.
func (fs *Filestore) Close() error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()
	if err := fs.queryIDStmt.Close(); err != nil {
		return fs.dbError(err)
	}
	if err := fs.insertFileStmt.Close(); err != nil {
		return fs.dbError(err)
	}
	if err := fs.insertVersionStmt.Close(); err != nil {
		return fs.dbError(err)
	}
	if err := fs.hasVersionStmt.Close(); err != nil {
		return fs.dbError(err)
	}
	if err := fs.db.Close(); err != nil {
		return fs.dbError(err)
	}
	return nil
}

func (fs *Filestore) dbError(err error) error {
	return fmt.Errorf("filestore DB error: %w", err)
}

func (fs *Filestore) dbPath() string {
	return fs.Root() + "db.sqlite3"
}

// Root returns the root directory, ending in a directory separator unless it is an
// empty relative directory (== the current directory).
func (fs *Filestore) Root() string {
	if fs.Dir == "" {
		return "versions/"
	}
	return asDirectoryPath(fs.Dir)
}

// Add adds a file with given path or updates the existing entries for the file.
// The file is versioned  and a version stored with the given info, tag strings and
// semantic version.
func (fs *Filestore) Add(path, info, version string) error {
	if fs.db == nil {
		return ErrNotOpen
	}
	check, err := fs.Checksum(path)
	if err != nil {
		return fmt.Errorf("filestore checksum failed for %s: %w", path, err)
	}
	return fs.addVersion(path, info, version, check)
}

func (fs *Filestore) addVersion(path, info, version, check string) error {
	name := filepath.Base(path)
	slashPath := filepath.ToSlash(path)
	rows, err := fs.queryIDStmt.Query(check)
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
		dst := fs.localPath(name, check)
		if err := ensureDirectory(filepath.Dir(dst), 0700); err != nil {
			return fmt.Errorf("filestore unable to create directory %s: %w", dst, err)
		}
		if flags.Has(fs.Options, Compress) {
			dst += ".snappy"
		}
		err := copyFile(path, dst, flags.Has(fs.Options, Compress), false)
		if err != nil {
			os.Remove(dst)
			return fmt.Errorf("filestore failed to copy file \"%s\" to %s: %w", name, dst, err)
		}
		result, err := fs.insertFileStmt.Exec(check)
		if err != nil {
			return fs.dbError(err)
		}
		fileID, err = result.LastInsertId()
		if err != nil {
			return fs.dbError(err)
		}
	}
	_, err = fs.insertVersionStmt.Exec(slashPath, info, EncodeMetaphone(info), version, fileID)
	return err
}

// localPath returns a local path in the root directory of the form
// root/checksum/name but with platform-specific separators.
func (fs *Filestore) localPath(name, checksum string) string {
	return fs.Root() + checksum + string(os.PathSeparator) + name
}

// Checksum computes a 512 byte Blake2b checksum of a given file.
func (fs *Filestore) Checksum(path string) (string, error) {
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
func (fs *Filestore) Has(file string) bool {
	if fs.db == nil {
		return false
	}
	var exists bool
	err := fs.hasVersionStmt.QueryRow(filepath.ToSlash(file)).Scan(&exists)
	if err != nil {
		return false
	}
	return exists
}

// FileVersion represents a particular version of a file.
type FileVersion struct {
	ID       int64     // file version ID (internal)
	Name     string    // the name of the file, including suffix
	Path     string    // the path from which the version was sourced (os path)
	Local    string    // the path to the file content on disk in the local filestore (os path)
	Info     string    // the info string
	Fuzzy    string    // fuzzy into string
	Version  string    // the version string
	From     time.Time // the datetime on which this version was added
	Checksum string    // the hex-encoded Blake2b checksum of the file contents of this version
}

// Get returns the latest version of a file at path, or an error if the file
// is not in the filestore.
func (fs *Filestore) Get(path string) (FileVersion, error) {
	if fs.db == nil {
		return FileVersion{}, ErrNotOpen
	}
	slashPath := filepath.ToSlash(path)
	row := fs.getVersionStmt.QueryRow(slashPath)
	v := FileVersion{}
	var timeStr string
	if err := row.Scan(&v.ID, &v.Path, &v.Info, &v.Fuzzy, &v.Version, &timeStr, &v.Checksum); err != nil {
		return FileVersion{}, fs.dbError(err)
	}
	var err error
	v.Name = filepath.Base(path)
	//	v.Path = filepath.FromSlash(v.Path)
	v.From, err = ParseDBDate(timeStr)
	if err != nil {
		return FileVersion{}, ErrInvalidDate
	}
	v.Local = fs.localPath(v.Name, v.Checksum)
	return v, nil
}

// Restore restores the given file version to destination directory dst.
func (fs *Filestore) Restore(version FileVersion, dst string) error {
	useCompression := flags.Has(fs.Options, Compress)
	dst = asDirectoryPath(dst)
	dstFile := dst + version.Name
	srcFile := fs.localPath(version.Name, version.Checksum)
	return copyFile(srcFile, dstFile, useCompression, true)
}

// RestoreAtSource restores the version into the original source destination path from which
// it was created. If a file already exists at this place (normally the case), it will be overwritten.
func (fs *Filestore) RestoreAtSource(version FileVersion) error {
	return fs.Restore(version, version.Path)
}

// Versions returns FileVersion entries for all versions of a file. Nil is returned if there are no versions.
func (fs *Filestore) Versions(path string, limit int) ([]FileVersion, error) {
	if fs.db == nil {
		return nil, ErrNotOpen
	}
	rows, err := fs.getVersionsStmt.Query(path, limit)
	if err != nil {
		return nil, fs.dbError(err)
	}
	return fs.getVersions(rows)
}

func (fs *Filestore) getVersions(rows *sql.Rows) ([]FileVersion, error) {
	versions := make([]FileVersion, 0)
	for rows.Next() {
		v := FileVersion{}
		var timeStr string
		if err := rows.Scan(&v.ID, &v.Path, &v.Info, &v.Fuzzy, &v.Version, &timeStr, &v.Checksum); err != nil {
			return nil, fs.dbError(err)
		}
		v.Path = filepath.FromSlash(v.Path)
		v.Name = filepath.Base(v.Path)
		var err error
		v.From, err = ParseDBDate(timeStr)
		if err != nil {
			return nil, ErrInvalidDate
		}
		v.Local = fs.localPath(v.Name, v.Checksum)
		versions = append(versions, v)
	}
	return versions, nil
}

// VersionsAfter returns FileVersion entries for all versions of a file after the given date. Nil
// is returned if there are no versions.
func (fs *Filestore) VersionsAfter(path string, after time.Time, limit int) ([]FileVersion, error) {
	if fs.db == nil {
		return nil, ErrNotOpen
	}
	rows, err := fs.getVersionsAfterStmt.Query(path, ToDBDate(after), limit)
	if err != nil {
		return nil, fs.dbError(err)
	}
	return fs.getVersions(rows)
}

// SimpleSearch returns FileVersion entries for all file info strings starting with terms, combined
// with OR but sorted from more to less matching entries.
func (fs *Filestore) SimpleSearch(words []string, limit int) ([]FileVersion, error) {
	if fs.db == nil {
		return nil, ErrNotOpen
	}
	term := ""
	for i, word := range words {
		if i > 0 {
			term += " or "
		}
		term += buildTerm("info", word)
		term += " or "
		term += buildTerm("version", word)
	}
	rows, err := fs.db.Query("select version_id, path, info, fuzzy, version, date, checksum from Versions inner join Files on Versions.file=Files.file_id where "+term+" order by date limit ?;", limit)
	if err != nil {
		return nil, err
	}
	return fs.getVersions(rows)
}

// search performs an FTS5 term search on the database directly. If fuzzy is true, the fuzzy info field is searched.
// Warning: Search terms are not escaped! To escape them, individual terms in a query
// must be put into double quotes and each double quote in a term must be turned into two double quotes "".
func (fs *Filestore) search(term string, limit int, fuzzy bool) ([]FileVersion, error) {
	if fs.db == nil {
		return nil, ErrNotOpen
	}
	var column string
	if fuzzy {
		column = "fuzzy"
	} else {
		column = "info"
	}
	rows, err := fs.db.Query("select version_id, path, info, fuzzy, version, date, checksum from VersionsFts inner join Files on VersionsFts.file=Files.file_id where "+column+" match ? or version match ? or date match ? order by date limit ?;", term, term, term, limit)
	if err != nil {
		return nil, err
	}
	return fs.getVersions(rows)
}

// Search performs an FTS5 term search on the database directly. This requires some knowledge of the database
// organization and FTS5 queries. Warning: Search terms are not escaped! To escape them, individual terms in a query
// must be put into double quotes and each double quote in a term must be turned into two double quotes "".
func (fs *Filestore) Search(term string, limit int) ([]FileVersion, error) {
	return fs.search(term, limit, false)
}

// FuzzySearch performs an FTS5 term search on the database directly. This requires some knowledge of the database
// organization and FTS5 queries. Warning: Search terms are not escaped! To escape them, individual terms in a query
// must be put into double quotes and each double quote in a term must be turned into two double quotes "".
func (fs *Filestore) FuzzySearch(term string, limit int) ([]FileVersion, error) {
	return fs.search(term, limit, true)
}

// buildTerm constructs a simple LIKE substring search query for one word
func buildTerm(column string, word string) string {
	word = safeReplacer.Replace(word)
	return fmt.Sprintf("%s like '%% %s' or %s like '%s %%' or %s like '%% %s %%' or %s like '%s'",
		column, word, column, word, column, word, column, word)
}

var safeReplacer = strings.NewReplacer("'", "", "%", "", ";", "", "\"", "", "\\", "")

var metaphoneEncoder = &metaphone3.Encoder{}

// EncodeMetaphone returns the primary metaphone version of text split up into words.
func EncodeMetaphone(text string) string {
	// text = strings.ToUpper(text)
	s := strings.Fields(text)
	m := make([]string, len(s))
	for i, word := range s {
		encoded, _ := metaphoneEncoder.Encode(word)
		m[i] = encoded
	}
	return strings.Join(m, " ")
}

// FTS5Escape escapes an individual FTS5 match query term in a safe way by enclosing it in quotes and turning quotes
// inside the term into sequences of two quotes. So, term"bla" is turned into "term""bla""", for instance.
func FTS5Escape(term string) string {
	return "\"" + strings.Replace(term, "\"", "\"\"", -1) + "\""
}

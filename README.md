# Filestore
a versioning local file storage system that avoids copying files

A local filestore allows you to copy files to a storage location on harddisk, keep track of changes manually (under your control), and retrieve older versions of files that have been stored. It stores unmodified copies and remembers metadata in an Sqlite3 database with FTS search.

## Usage

Not available yet. This package is in early alpha state and ought not be used.

## Dependencies

Filestore uses sqlite3 with the driver by mattn and this will not change. You probably shouldn't use it if you use another Sqlite driver.

## Implementation

The filestore is implemented following the K.I.S.S. principle. The source code should be easy to read and unsurprising. Please note that file changes are detected by computing a Blake2b checksum over whole files. This is not fast at all for large files and the Filestore ought not be used when performance is needed for checking whether a copy needs to be made. You should use a different solution with OS-level change tracking if you have this requirement.

If you find bugs or problems in a non-alpha version, please let me know by raising an issue.

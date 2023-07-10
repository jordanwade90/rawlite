package rawlite

import (
	"encoding/binary"
	"github.com/jordanwade90/rawlite/internal/pagebuf"
	"github.com/jordanwade90/rawlite/record"
	"io"
	"sync"
	"sync/atomic"
)

type schemaRecord struct {
	typ       string
	name      string
	tableName string
	rootPage  pagebuf.PageNumber
	sql       string
}

// Database represents a database file being created.
type Database struct {
	file           io.WriterAt
	nextPageNumber *atomic.Uint32

	// schemaLock protects schemaRecords and closed
	schemaLock    sync.Mutex
	schemaRecords []schemaRecord
	closed        bool
}

// OpenDatabase prepares to write a SQLite database to file.
func OpenDatabase(file io.WriterAt) *Database {
	db := &Database{
		file:           file,
		nextPageNumber: &atomic.Uint32{},
	}
	db.nextPageNumber.Store(2)
	return db
}

// Close writes the SQLite file header and the sqlite_schema table
// pointing to the root nodes of each Table and Index.
// It does not close the file the database was opened on.
func (db *Database) Close() error {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()

	if db.closed {
		panic("database closed")
	}
	db.closed = true

	hdr := pagebuf.NewDatabaseHeader(pageSize)

	// Simple case: everything fits in a single leaf node
	for i, entry := range db.schemaRecords {
		row, err := db.writeSchemaRecord(i, entry)
		if err != nil {
			return err
		}
		if !hdr.Add(row) {
			panic("not implemented")
		}
	}

	_, err := db.file.WriteAt(hdr.Finish(0), 0)
	return err
}

// OpenTable records schema information for an index
// and prepares the database for TableStreams to begin work.
func (db *Database) OpenTable() *Table {
	t := &Table{
		parent:       db,
		interiorPage: make([]byte, pageSize),
	}
	return t
}

// addSchemaRecord adds a row to the sqlite_schema table.
func (db *Database) addSchemaRecord(schema schemaRecord) {
	db.schemaLock.Lock()
	defer db.schemaLock.Unlock()

	if db.closed {
		panic("database closed")
	}

	db.schemaRecords = append(db.schemaRecords, schema)
}

func (db *Database) addTableSchemaRecord(name, sql string, rootPage pagebuf.PageNumber) {
	db.addSchemaRecord(schemaRecord{
		typ:       "table",
		name:      name,
		tableName: name,
		rootPage:  rootPage,
		sql:       sql,
	})
}

// allocPage allocates a page from the database file.
func (db *Database) allocPage() pagebuf.PageNumber {
	for {
		p := db.nextPageNumber.Add(1) - 1
		if p == 0 {
			panic("database too large")
		}
		if !isLockBytePage(p) {
			return pagebuf.PageNumber(p)
		}
	}
}

func isLockBytePage(pageNumber uint32) bool {
	return int64(pageNumber-1)*pageSize == 1073741824
}

func (db *Database) writePage(pageNumber pagebuf.PageNumber, page []byte) error {
	_, err := db.file.WriteAt(page, int64(pageNumber-1)*pageSize)
	return err
}

func (db *Database) writeOverflowPages(row []byte) (overflowPointer pagebuf.PageNumber, rowOnPage []byte, err error) {
	spaceRequired := tableLeafPayloadOnPage(pageSize, len(row))
	if len(row) > spaceRequired {
		page := make([]byte, pageSize)
		overflow := row[spaceRequired:]
		row = row[:spaceRequired]
		overflowPointer = db.allocPage()
		thisPage := overflowPointer
		nextPage := pagebuf.PageNumber(0)

		for len(overflow) > pageSize-4 {
			nextPage = db.allocPage()
			binary.BigEndian.PutUint32(page, uint32(nextPage))
			copy(page[4:], overflow)
			overflow = overflow[pageSize-4:]
			if err = db.writePage(thisPage, page); err != nil {
				return 0, nil, err
			}
			thisPage = nextPage
		}

		binary.BigEndian.PutUint32(page, uint32(nextPage))
		copy(page[4:], overflow)
		clear(page[4+len(overflow):])
		if err = db.writePage(thisPage, page); err != nil {
			return 0, nil, err
		}
	}
	return overflowPointer, row, nil
}

func (db *Database) writeSchemaRecord(rowid int, entry schemaRecord) (row []byte, err error) {
	rec := &record.Record{}
	rec.AppendString(entry.typ)
	rec.AppendString(entry.name)
	rec.AppendString(entry.tableName)
	rec.AppendUint(uint64(entry.rootPage))
	rec.AppendString(entry.sql)

	payload := rec.AppendTo(nil)
	payloadLen := len(payload)
	overflowPointer, payload, err := db.writeOverflowPages(payload)
	if err != nil {
		return nil, err
	}
	return appendTableRow(nil, int64(payloadLen), int64(rowid+1), payload, overflowPointer), nil
}

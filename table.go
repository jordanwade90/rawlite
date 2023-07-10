package rawlite

import (
	"encoding/binary"
	"github.com/jordanwade90/rawlite/internal/pagebuf"
	"github.com/jordanwade90/rawlite/internal/svarint"
	"sync"
)

const (
	pageSize       = 65536
	minRowSize     = 4
	maxRowsPerPage = pageSize / minRowSize
)

// Table represents a table being created.
type Table struct {
	parent *Database

	// interiorLock protects interiorNodes.
	interiorLock  sync.Mutex
	interiorNodes []*pagebuf.TableInterior
	interiorPage  []byte
	closed        bool
}

// OpenStream opens a TableStream for writing to this table.
func (tbl *Table) OpenStream() *TableStream {
	return &TableStream{
		parent: tbl,
		page:   pagebuf.NewTableLeaf(pageSize),
		cell:   make([]byte, 0, pageSize),
	}
}

// Close closes the B-tree and informs the Database of the root page number.
func (tbl *Table) Close(name, sql string) error {
	tbl.interiorLock.Lock()
	defer tbl.interiorLock.Unlock()

	if tbl.closed {
		panic("table closed")
	}
	tbl.closed = true

	for i := 0; i < len(tbl.interiorNodes); i++ {
		node := tbl.interiorNodes[i]
		if node.Length() == 1 {
			rootPage, _ := node.Remove()
			tbl.parent.addTableSchemaRecord(name, sql, rootPage)
			return nil
		}

		for {
			pageNum := tbl.parent.allocPage()
			rightmostRowid, empty := node.Put(tbl.interiorPage)
			if err := tbl.parent.writePage(pageNum, tbl.interiorPage); err != nil {
				return err
			}

			if i+1 == len(tbl.interiorNodes) {
				if empty {
					// We just wrote the root page.
					tbl.parent.addTableSchemaRecord(name, sql, pageNum)
					return nil
				}

				tbl.interiorNodes = append(tbl.interiorNodes, pagebuf.NewTableInterior(pageSize))
			}
			tbl.interiorNodes[i+1].Add(pageNum, rightmostRowid)

			if empty {
				break
			}
		}
	}

	// If there were no interior nodes the table must be empty.
	rootPage := tbl.parent.allocPage()
	tbl.parent.addTableSchemaRecord(name, sql, rootPage)
	return tbl.parent.writePage(rootPage, pagebuf.NewTableLeaf(pageSize).Finish())
}

func (tbl *Table) allocRowidBlock() (int64, error) {
	tbl.interiorLock.Lock()
	defer tbl.interiorLock.Unlock()

	if tbl.closed {
		panic("table closed")
	}

	if len(tbl.interiorNodes) == 0 {
		tbl.interiorNodes = append(tbl.interiorNodes, pagebuf.NewTableInterior(pageSize))
	}

	pageNum := tbl.parent.allocPage()
	firstRowid := int64(pageNum) * maxRowsPerPage
	rightmostRowid := firstRowid + maxRowsPerPage - 1

	for i := 0; i < len(tbl.interiorNodes); i++ {
		if tbl.interiorNodes[i].Add(pageNum, rightmostRowid) {
			return firstRowid, nil
		}

		pageNum = tbl.parent.allocPage()
		rightmostRowid, _ = tbl.interiorNodes[i].Put(tbl.interiorPage)
		if err := tbl.parent.writePage(pageNum, tbl.interiorPage); err != nil {
			return 0, err
		}
	}

	tbl.interiorNodes = append(tbl.interiorNodes, pagebuf.NewTableInterior(pageSize))
	tbl.interiorNodes[len(tbl.interiorNodes)-1].Add(pageNum, rightmostRowid)
	return firstRowid, nil
}

func (tbl *Table) writeLeaf(lastRowid int64, page []byte) error {
	childPointer := pagebuf.PageNumber(lastRowid / maxRowsPerPage)
	return tbl.parent.writePage(childPointer, page)
}

// TableStream represents one stream of data being written to a Table.
// TableStreams are not thread-safe; open one TableStream per worker goroutine.
type TableStream struct {
	parent *Table
	// page helps write leaf pages
	page *pagebuf.TableLeaf
	// cell is a reusable buffer for formatting cells
	cell []byte
	// The rowid of the next cell written.
	nextRowid int64
}

// Close informs the parent Table that this TableStream is finished writing,
// passing it any bookkeeping information required to construct the B-tree.
func (s *TableStream) Close() error {
	return s.Flush()
}

// Flush flushes any buffered pages.
// WriteRow will begin with a new page if called after Flush.
func (s *TableStream) Flush() error {
	if s.page.IsEmpty() {
		return nil
	}

	err := s.parent.writeLeaf(s.nextRowid-1, s.page.Finish())
	s.nextRowid = 0
	return err
}

// WriteRow writes one row to the table whose contents are row,
// returning the rowid assigned to the row
// and any error resulting from writing pages to the database.
//
// WriteRow does not retain row.
func (s *TableStream) WriteRow(row []byte) (rowid int64, err error) {
	if s.nextRowid == 0 {
		if s.nextRowid, err = s.parent.allocRowidBlock(); err != nil {
			return 0, err
		}
	}

	payloadLen := len(row)
	overflowPointer, row, err := s.parent.parent.writeOverflowPages(row)
	if err != nil {
		return 0, err
	}

	for {
		rowid = s.nextRowid
		s.cell = appendTableRow(s.cell[:0], int64(payloadLen), rowid, row, overflowPointer)
		if s.page.Add(s.cell) {
			s.nextRowid++
			return
		}
		if err = s.Flush(); err != nil {
			return 0, err
		}
		if s.nextRowid, err = s.parent.allocRowidBlock(); err != nil {
			return 0, err
		}
	}
}

func appendTableRow(buf []byte, payloadLen, rowid int64, row []byte, overflowPointer pagebuf.PageNumber) []byte {
	buf = svarint.Append(buf, uint64(payloadLen))
	buf = svarint.Append(buf, uint64(rowid))
	buf = append(buf, row...)
	if overflowPointer != 0 {
		buf = binary.BigEndian.AppendUint32(buf, uint32(overflowPointer))
	}
	return buf
}

func tableLeafPayloadOnPage(pageSize int, payloadSize int) int {
	// See the "alternative description" of the payload overflow calculation
	// from https://sqlite.org/fileformat2.html
	X := pageSize - 35
	M := ((pageSize - 12) * 32 / 255) - 23
	K := M + ((payloadSize - M) % (pageSize - 4))
	switch {
	case payloadSize <= X:
		return payloadSize
	case K <= X:
		return K
	default:
		return M
	}
}

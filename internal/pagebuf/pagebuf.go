package pagebuf

import (
	"encoding/binary"
	"github.com/jordanwade90/rawlite/internal/svarint"
)

const (
	DatabaseHeaderSize      = 100
	TableLeafHeaderSize     = 8
	TableInteriorHeaderSize = 12
)

// PageNumber annotates uint32s that are actually page numbers.
type PageNumber uint32

type tablePage struct {
	page         []byte
	contentStart int
	numCells     int
	headerSize   int
}

func (p *tablePage) Add(cell []byte) bool {
	// We write back-to-front, so, confusingly,
	// contentStart should be the larger number.
	contentStart := p.contentStart - len(cell)
	contentEnd := p.headerSize + 2*p.numCells
	if contentStart < contentEnd+2 {
		return false
	}

	binary.BigEndian.PutUint16(p.page[contentEnd:], uint16(contentStart))
	copy(p.page[contentStart:], cell)
	p.contentStart = contentStart
	p.numCells++
	return true
}

func (p *tablePage) Checkpoint() [2]int {
	return [2]int{p.contentStart, p.numCells}
}

func (p *tablePage) Restore(checkpoint [2]int) {
	p.contentStart, p.numCells = checkpoint[0], checkpoint[1]
}

// TableLeaf helps write table B-tree leaf nodes.
type TableLeaf tablePage

// NewTableLeaf returns an empty TableLeaf.
func NewTableLeaf(pageSize int) *TableLeaf {
	return &TableLeaf{
		page:         make([]byte, pageSize),
		contentStart: pageSize,
		headerSize:   TableLeafHeaderSize,
	}
}

// Add tries to add a cell to a TableLeaf, returning true if it fits.
func (p *TableLeaf) Add(cell []byte) bool { return (*tablePage)(p).Add(cell) }

// Finish finishes writing the node, returning a page-sized slice with its contents.
// The TableLeaf is emptied and ready to reuse after Finish returns.
//
// Note that Finish returns a reference to the TableLeaf's internal buffer;
// do not modify the return value.
func (p *TableLeaf) Finish() []byte {
	p.page[0] = 13
	p.page[1] = 0
	p.page[2] = 0
	binary.BigEndian.PutUint16(p.page[3:], uint16(p.numCells))
	binary.BigEndian.PutUint16(p.page[5:], uint16(p.contentStart))
	p.page[7] = 0

	p.contentStart = len(p.page)
	p.numCells = 0
	return p.page
}

// IsEmpty returns whether the TableLeaf is empty.
func (p *TableLeaf) IsEmpty() bool { return p.numCells == 0 }

// TableInterior helps write table B-tree interior nodes.
type TableInterior struct {
	pageNumbers  []PageNumber
	rowids       []int64
	pageSize     int
	contentStart int
	excessCells  int
}

// NewTableInterior returns an empty TableInterior.
func NewTableInterior(pageSize int) *TableInterior {
	return &TableInterior{
		pageSize:     pageSize,
		contentStart: pageSize,
	}
}

func (ti *TableInterior) updateBookkeeping(numCells int, cellLen int) {
	if ti.excessCells == 0 {
		contentStart := ti.contentStart - cellLen
		contentEnd := TableInteriorHeaderSize + 2*numCells + 2
		if contentStart < contentEnd {
			ti.excessCells = 1
		} else {
			ti.contentStart = contentStart
		}
	} else {
		ti.excessCells++
	}
}

// Add adds a cell to a TableInterior.
// If Add returns false, a full page of cells has been buffered;
// call Put to write the page and make room for more.
func (ti *TableInterior) Add(pageNumber PageNumber, rowid int64) (ok bool) {
	ti.pageNumbers = append(ti.pageNumbers, pageNumber)
	ti.rowids = append(ti.rowids, rowid)
	ti.updateBookkeeping(len(ti.pageNumbers), 4+svarint.Length(rowid))
	return ti.excessCells < 2
}

// Length returns the number of children in the node, including excess cells.
func (ti *TableInterior) Length() int {
	return len(ti.pageNumbers)
}

// Put writes an interior B-tree page to p and removes all cells used from the buffer.
//
// If the table is open, call Put once whenever Add returns false and ignore empty.
// If the table has been closed, keep calling Put until empty is true.
func (ti *TableInterior) Put(p []byte) (rightmostRowid int64, empty bool) {
	if len(ti.pageNumbers) < 2 {
		panic("degenerate node")
	}

	contentStart := len(p)
	numCells := 0
	limit := len(ti.pageNumbers) - ti.excessCells
	if ti.excessCells == 1 {
		limit--
	}

	for numCells < limit-1 {
		contentStart -= 4 + svarint.Length(ti.rowids[numCells])
		contentEnd := TableInteriorHeaderSize + 2*numCells + 2
		if contentStart <= contentEnd {
			// NOTE(jw): either Add messed up the contentStart/excessCells bookkeeping
			// or Put messed up the cell offsets.
			panic("internal bug")
		}

		binary.BigEndian.PutUint16(p[contentEnd-2:], uint16(contentStart))
		binary.BigEndian.PutUint32(p[contentStart:], uint32(ti.pageNumbers[numCells]))
		svarint.Put(p[contentStart+4:], ti.rowids[numCells])
		numCells++
	}
	rightmostRowid = ti.rowids[numCells]

	p[0] = 5
	p[1] = 0
	p[2] = 0
	binary.BigEndian.PutUint16(p[3:], uint16(numCells))
	binary.BigEndian.PutUint16(p[5:], uint16(contentStart))
	p[7] = 0
	binary.BigEndian.PutUint32(p[8:], uint32(ti.pageNumbers[numCells]))

	ti.pageNumbers = append(ti.pageNumbers[:0], ti.pageNumbers[numCells+1:]...)
	ti.rowids = append(ti.rowids[:0], ti.rowids[numCells+1:]...)
	ti.contentStart = ti.pageSize
	ti.excessCells = 0
	for i := 0; i < len(ti.pageNumbers); i++ {
		ti.updateBookkeeping(i, 4+svarint.Length(ti.rowids[i]))
	}

	return rightmostRowid, len(ti.pageNumbers) == 0
}

// Remove removes the most recent cell added with Add.
func (ti *TableInterior) Remove() (pageNumber PageNumber, rowid int64) {
	if len(ti.pageNumbers) == 0 {
		panic("empty node")
	}

	pageNumber, rowid = ti.pageNumbers[len(ti.pageNumbers)-1], ti.rowids[len(ti.rowids)-1]
	ti.pageNumbers = ti.pageNumbers[:len(ti.pageNumbers)-1]
	ti.rowids = ti.rowids[:len(ti.rowids)-1]

	if ti.excessCells > 0 {
		ti.excessCells--
	} else {
		ti.contentStart += 4 + svarint.Length(rowid)
	}

	return
}

// DatabaseHeader helps write the database header page
type DatabaseHeader tablePage

// NewDatabaseHeader returns an empty DatabaseHeader.
// The schema root page is configured to be a leaf node;
// to make it be an interior node, call Promote.
func NewDatabaseHeader(pageSize int) *DatabaseHeader {
	return &DatabaseHeader{
		page:         make([]byte, pageSize),
		contentStart: pageSize,
		headerSize:   DatabaseHeaderSize + TableLeafHeaderSize,
	}
}

// Add tries to add a cell to a DatabaseHeader, returning true if it fits.
func (p *DatabaseHeader) Add(cell []byte) bool { return (*tablePage)(p).Add(cell) }

// Finish finishes writing the node, returning a page-sized slice with its contents.
// The DatabaseHeader is emptied and ready to reuse after Finish returns.
//
// If the root page is an interior node (Promote was called),
// rightMostPointer must be nonzero.
// If the root page is a leaf node, rightMostPointer must be zero.
//
// Note that Finish returns a reference to the DatabaseHeader's internal buffer;
// do not modify the return value.
func (p *DatabaseHeader) Finish(rightMostPointer uint32) []byte {
	// Database header
	copy(p.page, "SQLite format 3\000")
	if len(p.page) == 65536 {
		binary.BigEndian.PutUint32(p.page[16:], 0x010101)
	} else {
		binary.BigEndian.PutUint32(p.page[16:], uint32(len(p.page)<<16)|0x0101)
	}
	binary.BigEndian.PutUint32(p.page[20:], 0x00402020)
	binary.BigEndian.PutUint32(p.page[44:], 4)
	binary.BigEndian.PutUint32(p.page[48:], uint32(2048000/len(p.page)))
	binary.BigEndian.PutUint32(p.page[56:], 1)
	binary.BigEndian.PutUint32(p.page[96:], 3003000)

	// sqlite_schema root page header
	if rightMostPointer != 0 {
		p.page[DatabaseHeaderSize] = 5
	} else {
		p.page[DatabaseHeaderSize] = 13
	}
	p.page[DatabaseHeaderSize+1] = 0
	p.page[DatabaseHeaderSize+2] = 0
	binary.BigEndian.PutUint16(p.page[DatabaseHeaderSize+3:], uint16(p.numCells))
	binary.BigEndian.PutUint16(p.page[DatabaseHeaderSize+5:], uint16(p.contentStart))
	p.page[DatabaseHeaderSize+7] = 0
	if rightMostPointer != 0 {
		binary.BigEndian.PutUint32(p.page[DatabaseHeaderSize+8:], rightMostPointer)
	}

	p.contentStart = len(p.page)
	p.numCells = 0

	return p.page
}

// Promote clears the node contents
// and reconfigures the schema root page to be an interior node.
func (p *DatabaseHeader) Promote() {
	panic("broken")
	p.contentStart = len(p.page)
	p.numCells = 0
	p.headerSize = DatabaseHeaderSize + TableInteriorHeaderSize
}

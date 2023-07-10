// Package rawlite implements a library to write SQLite databases with a streaming API
// for quickly ingesting large amounts of data into SQLite.
//
// First, skim the description of SQLite's architecture at https://sqlite.org/arch.html.
//
// In terms of SQLite's architecture,
// this library abstracts the OS interface, pager, and part of the B-tree layer.
// Its performance comes from using multiple threads to generate leaf nodes,
// only requiring a single-threaded fixup phase at the end
// to generate interior nodes and the SQLite file header.
// The tradeoff is that the user must cooperate with the library
// to generate those leaf nodes in such a way
// that it can assemble them into a valid B-tree.
//
// Now, read the description of the SQLite file format at https://sqlite.org/fileformat2.html.
// This library abstracts some details of the file format while deliberately exposing others,
// which is the key to its performance.
// An understanding of the file format is essential to use the library correctly.
//
// B-trees are abstracted into a stream of cells.
// A TableStream generates a stream of leaf nodes for the cells written into it.
// The performance comes from using many of them in parallel,
// each operating independently,
// arranging the data in such a way that these independent streams may be merged at the end.
// Closing a Table creates the interior nodes of the B-tree.
// Closing a Database creates the `sqlite_schema` table pointing to the root nodes of each table.
//
// Page allocation, B-tree interior nodes, and overflow pages for large cells are abstracted,
// but it is still required to format cells correctly
// and to ensure that it is possible to combine the
// independently-generated streams of leaf nodes into a valid B-tree.
// The library guarantees this for TableStream by internally generating rowids
// for each cell in such a way that guarantees a valid B-tree can be formed.
package rawlite

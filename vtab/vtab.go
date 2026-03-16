package vtab

import (
	"database/sql"
	"database/sql/driver"
	"errors"
)

// Value is the value type passed to and from virtual table cursors. It
// aliases database/sql/driver.Value to avoid exposing low-level details to
// module authors while remaining compatible with the driver.
type Value = driver.Value

// Blob represents an open BLOB handle for direct read/write access to a BLOB column.
// It wraps the SQLite sqlite3_blob* handle and provides efficient binary I/O.
//
// Blob is NOT safe for concurrent use. Each Blob should be used by a single goroutine
// and must be closed after use. The typical usage pattern is within a single callback
// or transaction scope.
type Blob struct {
	handle uintptr
	read   func(handle uintptr, offset int64, p []byte) error
	write  func(handle uintptr, offset int64, p []byte) error
	close  func(handle uintptr) error
}

// NewBlob creates a new Blob with the given handle and operations.
// This is intended for use by the engine to create Blob instances.
func NewBlob(handle uintptr, read func(uintptr, int64, []byte) error, write func(uintptr, int64, []byte) error, close func(uintptr) error) *Blob {
	return &Blob{handle: handle, read: read, write: write, close: close}
}

// Read reads data from the BLOB at the given offset into p.
// Returns the number of bytes read, which should always be len(p) on success.
func (b *Blob) Read(offset int64, p []byte) (int, error) {
	if b.handle == 0 {
		return 0, errors.New("vtab: blob handle is closed")
	}
	if err := b.read(b.handle, offset, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Write writes data to the BLOB at the given offset from p.
// Returns the number of bytes written, which should always be len(p) on success.
func (b *Blob) Write(offset int64, p []byte) (int, error) {
	if b.handle == 0 {
		return 0, errors.New("vtab: blob handle is closed")
	}
	if err := b.write(b.handle, offset, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close closes the BLOB handle and releases resources.
// After Close, all operations on the Blob will return an error.
func (b *Blob) Close() error {
	if b.handle == 0 {
		return nil
	}
	err := b.close(b.handle)
	b.handle = 0
	return err
}

// Context carries information that a Module may need when creating or
// connecting a table instance. It intentionally does not expose *sql.DB to
// avoid leaking database/sql internals into the vtab API. Additional fields
// may be added in the future as needed.
type Context struct {
	declare func(string) error
	// constraintSupport enables constraint support (e.g. MATCH) for the module.
	constraintSupport func() error
	// config issues sqlite3_vtab_config calls for other vtab options.
	config func(op int32, args ...int32) error
	// execSQL executes SQL using the underlying SQLite connection.
	// This is optional; if nil, Exec() will return an error.
	execSQL func(sql string, args []driver.Value) error
	// openBlob opens a BLOB handle for direct read/write access.
	openBlob func(db, table, column string, rowid int64, write bool) (*Blob, error)
	// noChangeCheck checks if a column value is unchanged during UPDATE.
	// Used by ValueNoChange. Optional; returns false if not available.
	noChangeCheck func(colIndex int) bool
	// inFirst returns the first value in an IN constraint list.
	// Returns (Value, true) if available, (nil, false) otherwise.
	inFirst func(valPtr uintptr) (Value, bool)
	// inNext returns the next value in an IN constraint list.
	// Returns (Value, true) if more values, (nil, false) at end.
	inNext func(valPtr uintptr) (Value, bool)
	// valPtrs stores the raw sqlite3_value pointers for the current operation.
	// Used by IN iteration. May be nil if not applicable.
	valPtrs []uintptr
}

// Declare must be called by a module from within Create or Connect to declare
// the schema of the virtual table. The provided SQL must be a CREATE TABLE
// statement describing the exposed columns.
//
// The engine installs this callback so that the declaration is executed in the
// correct context. Calling Declare outside of Create/Connect may fail.
func (c Context) Declare(schema string) error {
	if c.declare == nil {
		return errors.New("vtab: declare not available in this context")
	}
	return c.declare(schema)
}

// EnableConstraintSupport enables virtual table constraint support in SQLite.
// This must be called from within Create or Connect.
func (c Context) EnableConstraintSupport() error {
	if c.constraintSupport == nil {
		return errors.New("vtab: constraint support not available in this context")
	}
	return c.constraintSupport()
}

// Config forwards sqlite3_vtab_config options to SQLite. This must be called
// from within Create or Connect.
func (c Context) Config(op int32, args ...int32) error {
	if c.config == nil {
		return errors.New("vtab: config not available in this context")
	}
	return c.config(op, args...)
}

// Exec executes a SQL statement within the current transaction.
// Unlike *sql.DB.Exec(), this does not cause deadlock during vtab callbacks
// because it uses the same underlying SQLite connection/session.
//
// Use cases include:
//   - Creating shadow tables during Create/Connect
//   - Syncing auxiliary data during Update/Insert/Delete
//   - Maintaining indexes or materialized views
//
// Note: This method is available only when the engine provides transaction
// context support. If Exec is called when not available, it returns an error.
// Modules should check the error and gracefully degrade if needed.
func (c Context) Exec(sql string, args ...driver.Value) error {
	if c.execSQL == nil {
		return errors.New("vtab: Exec not available in this context")
	}
	return c.execSQL(sql, args)
}

// OpenBlob opens a BLOB for direct read/write access.
// This is more efficient than using SQL for binary data operations,
// especially for large BLOBs or when modifying only portions of a BLOB.
//
// Parameters:
//   - db: database name (typically "main")
//   - table: table name containing the BLOB column
//   - column: column name of the BLOB
//   - rowid: rowid of the row containing the BLOB
//   - write: true for read/write access, false for read-only
//
// Returns a Blob handle that must be closed after use.
//
// Example:
//
//	blob, err := ctx.OpenBlob("main", "chunks", "vector_data", rowid, true)
//	if err != nil { return err }
//	defer blob.Close()
//	blob.Write(offset, vectorData)
func (c Context) OpenBlob(db, table, column string, rowid int64, write bool) (*Blob, error) {
	if c.openBlob == nil {
		return nil, errors.New("vtab: OpenBlob not available in this context")
	}
	return c.openBlob(db, table, column, rowid, write)
}

// ValueNoChange checks if the column at the given index is unchanged during
// an UPDATE operation. This is useful for optimizing updates by skipping
// columns that haven't been modified.
//
// Returns true if the column value is unchanged, false if changed or if
// the check is not available (e.g., during INSERT or when called outside
// of an UPDATE callback).
//
// Example:
//
//	func (t *MyTable) Update(ctx Context, oldRowid int64, cols []Value, newRowid *int64) error {
//	    for i, col := range cols {
//	        if ctx.ValueNoChange(i) {
//	            continue // Skip unchanged column
//	        }
//	        // Process changed column
//	    }
//	}
func (c Context) ValueNoChange(colIndex int) bool {
	if c.noChangeCheck == nil || colIndex < 0 {
		return false
	}
	return c.noChangeCheck(colIndex)
}

// InIterator provides iteration over IN constraint values.
// It is returned by Context.InIterate to iterate through the elements
// of an IN(...) expression.
type InIterator struct {
	valPtr  uintptr
	inFirst func(uintptr) (Value, bool)
	inNext  func(uintptr) (Value, bool)
	current Value
	done    bool
}

// Next advances the iterator. Returns true if there are more values.
// The current value can be accessed via Value() after a successful Next().
func (it *InIterator) Next() bool {
	if it.done {
		return false
	}
	if it.current == nil {
		// First call
		if it.inFirst == nil {
			it.done = true
			return false
		}
		v, ok := it.inFirst(it.valPtr)
		if !ok {
			it.done = true
			return false
		}
		it.current = v
		return true
	}
	// Subsequent calls
	if it.inNext == nil {
		it.done = true
		return false
	}
	v, ok := it.inNext(it.valPtr)
	if !ok {
		it.done = true
		return false
	}
	it.current = v
	return true
}

// Value returns the current value in the iteration.
// Must be called after Next() returns true.
func (it *InIterator) Value() Value {
	return it.current
}

// InIterate creates an iterator for an IN constraint value.
// The argIndex corresponds to the position in the Values slice passed
// to Filter (0-based), which should have been identified as an IN
// constraint during BestIndex.
//
// Example:
//
//	func (c *MyCursor) Filter(idxNum int, idxStr string, vals []Value) error {
//	    // Assuming constraint at index 0 is an IN constraint
//	    it := ctx.InIterate(vals, 0)
//	    for it.Next() {
//	        v := it.Value()
//	        // Process each value in the IN list
//	    }
//	}
func (c Context) InIterate(vals []Value, argIndex int) *InIterator {
	if c.inFirst == nil || c.valPtrs == nil || argIndex < 0 || argIndex >= len(c.valPtrs) {
		return &InIterator{done: true}
	}
	return &InIterator{
		valPtr:  c.valPtrs[argIndex],
		inFirst: c.inFirst,
		inNext:  c.inNext,
		done:    false,
	}
}

// NewContext is used by the engine to create a Context bound to the current
// xCreate/xConnect call. External modules should not need to call this.
func NewContext(declare func(string) error) Context { return Context{declare: declare} }

// NewContextWithConstraintSupport is used by the engine to create a Context
// that can enable constraint support.
func NewContextWithConstraintSupport(declare func(string) error, constraintSupport func() error) Context {
	return Context{declare: declare, constraintSupport: constraintSupport}
}

// NewContextWithConfig is used by the engine to create a Context that can
// enable constraint support and other sqlite3_vtab_config options.
func NewContextWithConfig(declare func(string) error, constraintSupport func() error, config func(op int32, args ...int32) error) Context {
	return Context{declare: declare, constraintSupport: constraintSupport, config: config}
}

// NewContextWithExec creates a Context with full functionality including
// the ability to execute SQL within the current transaction.
// This should be used for callbacks that need to perform additional SQL
// operations (e.g., creating shadow tables, syncing auxiliary data).
func NewContextWithExec(
	declare func(string) error,
	constraintSupport func() error,
	config func(op int32, args ...int32) error,
	execSQL func(sql string, args []driver.Value) error,
) Context {
	return Context{
		declare:           declare,
		constraintSupport: constraintSupport,
		config:            config,
		execSQL:           execSQL,
	}
}

// BlobOps contains the operations for BLOB access.
type BlobOps struct {
	Open  func(db, table, column string, rowid int64, write bool) (*Blob, error)
	Read  func(handle uintptr, offset int64, p []byte) error
	Write func(handle uintptr, offset int64, p []byte) error
	Close func(handle uintptr) error
}

// NewContextWithBlob creates a Context with BLOB access capabilities.
// This is the most feature-complete constructor for modules that need
// both SQL execution and efficient BLOB I/O.
func NewContextWithBlob(
	declare func(string) error,
	constraintSupport func() error,
	config func(op int32, args ...int32) error,
	execSQL func(sql string, args []driver.Value) error,
	ops *BlobOps,
) Context {
	return Context{
		declare:           declare,
		constraintSupport: constraintSupport,
		config:            config,
		execSQL:           execSQL,
		openBlob: func(db, table, column string, rowid int64, write bool) (*Blob, error) {
			return ops.Open(db, table, column, rowid, write)
		},
	}
}

// UpdateContextConfig contains configuration for creating a Context
// used during xUpdate callbacks. This enables nochange detection.
type UpdateContextConfig struct {
	NoChangeCheck func(colIndex int) bool
}

// NewContextForUpdate creates a Context for xUpdate callbacks with
// nochange detection support.
func NewContextForUpdate(base Context, cfg *UpdateContextConfig) Context {
	base.noChangeCheck = cfg.NoChangeCheck
	return base
}

// FilterContextConfig contains configuration for creating a Context
// used during xFilter callbacks. This enables IN constraint iteration.
type FilterContextConfig struct {
	ValPtrs []uintptr                   // Raw sqlite3_value pointers
	InFirst func(uintptr) (Value, bool) // IN iteration start
	InNext  func(uintptr) (Value, bool) // IN iteration next
}

// NewContextForFilter creates a Context for xFilter callbacks with
// IN constraint iteration support.
func NewContextForFilter(base Context, cfg *FilterContextConfig) Context {
	base.valPtrs = cfg.ValPtrs
	base.inFirst = cfg.InFirst
	base.inNext = cfg.InNext
	return base
}

// Module represents a virtual table module, analogous to sqlite3_module in
// the SQLite C API. Implementations are responsible for creating and
// connecting table instances.
type Module interface {
	// Create is called to create a new virtual table. args corresponds to the
	// argv array passed to xCreate in the SQLite C API: it contains the module
	// name, the database name, the table name, and module arguments.
	Create(ctx Context, args []string) (Table, error)

	// Connect is called to connect to an existing virtual table. Its
	// semantics mirror xConnect in the SQLite C API.
	Connect(ctx Context, args []string) (Table, error)
}

// Table represents a single virtual table instance (the Go analogue of
// sqlite3_vtab and its associated methods).
type Table interface {
	// BestIndex allows the virtual table to inform SQLite about which
	// constraints and orderings it can efficiently support. The IndexInfo
	// structure mirrors sqlite3_index_info.
	BestIndex(info *IndexInfo) error

	// Open creates a new cursor for scanning the table.
	Open() (Cursor, error)

	// Disconnect is called to disconnect from a table instance (xDisconnect).
	Disconnect() error

	// Destroy is called when a table is dropped (xDestroy).
	Destroy() error
}

// Renamer can be implemented by a Table to handle xRename.
type Renamer interface {
	Rename(newName string) error
}

// Transactional can be implemented by a Table to handle transaction-related
// callbacks. Methods are optional; unimplemented methods are treated as no-op.
type Transactional interface {
	Begin() error
	Sync() error
	Commit() error
	Rollback() error
	Savepoint(i int) error
	Release(i int) error
	RollbackTo(i int) error
}

// TransactionalWithContext can be implemented by a Table to handle transaction-related
// callbacks with access to Context for executing SQL and BLOB operations.
// This enables modules to perform efficient I/O during transaction commits.
//
// When both Transactional and TransactionalWithContext are implemented,
// TransactionalWithContext takes precedence.
//
// Example:
//
//	func (t *MyTable) Commit(ctx Context) error {
//	    // Use ctx.Exec() or ctx.OpenBlob() for efficient operations
//	    blob, _ := ctx.OpenBlob("main", "shadow", "data", rowid, true)
//	    defer blob.Close()
//	    blob.Write(0, data)
//	    return nil
//	}
type TransactionalWithContext interface {
	Begin(ctx Context) error
	Sync(ctx Context) error
	Commit(ctx Context) error
	Rollback(ctx Context) error
	Savepoint(ctx Context, i int) error
	Release(ctx Context, i int) error
	RollbackTo(ctx Context, i int) error
}

// Cursor represents a cursor over a virtual table (sqlite3_vtab_cursor).
type Cursor interface {
	// Filter corresponds to xFilter. idxNum and idxStr are the chosen index
	// number and string; vals are the constraint arguments.
	Filter(idxNum int, idxStr string, vals []Value) error

	// Next advances the cursor to the next row (xNext).
	Next() error

	// Eof reports whether the cursor is past the last row (xEof != 0).
	Eof() bool

	// Column returns the value of the specified column in the current row
	// (xColumn).
	Column(col int) (Value, error)

	// Rowid returns the current rowid (xRowid).
	Rowid() (int64, error)

	// Close closes the cursor (xClose).
	Close() error
}

// CursorWithContext can be implemented by a Cursor to handle xFilter
// with access to Context for IN constraint iteration and other capabilities.
//
// When both Cursor and CursorWithContext are implemented, the FilterWithContext
// method is preferred. This enables:
//   - IN constraint iteration via ctx.InIterate()
//   - Efficient handling of IN (...) expressions
//
// Example:
//
//	func (c *MyCursor) FilterWithContext(ctx Context, idxNum int, idxStr string, vals []Value) error {
//	    // Iterate over IN constraint values
//	    it := ctx.InIterate(vals, 0)
//	    for it.Next() {
//	        v := it.Value()
//	        // Process each value
//	    }
//	    return nil
//	}
type CursorWithContext interface {
	FilterWithContext(ctx Context, idxNum int, idxStr string, vals []Value) error
	Next() error
	Eof() bool
	Column(col int) (Value, error)
	Rowid() (int64, error)
	Close() error
}

// Updater can be implemented by a Table to support writes via xUpdate.
//
// Semantics follow SQLite's xUpdate:
//   - Delete: Delete(oldRowid) is called.
//   - Insert: Insert(cols, rowid) is called. *rowid may contain a desired rowid
//     (if provided by SQL) and should be set to the final rowid of the new row.
//   - Update: Update(oldRowid, cols, newRowid) is called. *newRowid may be set
//     to the final rowid of the updated row when changed.
type Updater interface {
	Insert(cols []Value, rowid *int64) error
	Update(oldRowid int64, cols []Value, newRowid *int64) error
	Delete(oldRowid int64) error
}

// UpdaterWithContext can be implemented by a Table to support writes via xUpdate
// with access to Context for executing SQL and BLOB operations.
//
// This interface enables modules to:
//   - Sync shadow tables during Insert/Update/Delete using Context.Exec
//   - Directly write binary data to shadow tables using Context.OpenBlob
//   - Achieve better performance by avoiding intermediate SQL parsing
//
// When both Updater and UpdaterWithContext are implemented, UpdaterWithContext
// takes precedence.
//
// Semantics follow SQLite's xUpdate:
//   - Delete: Delete(ctx, oldRowid) is called.
//   - Insert: Insert(ctx, cols, rowid) is called.
//   - Update: Update(ctx, oldRowid, cols, newRowid) is called.
type UpdaterWithContext interface {
	Insert(ctx Context, cols []Value, rowid *int64) error
	Update(ctx Context, oldRowid int64, cols []Value, newRowid *int64) error
	Delete(ctx Context, oldRowid int64) error
}

// ConstraintOp describes the operator used in a constraint on a virtual
// table column. It loosely mirrors the op field of sqlite3_index_constraint.
type ConstraintOp int

const (
	// OpUnknown indicates an operator that is not recognized or not mapped.
	// Modules should treat this conservatively.
	OpUnknown ConstraintOp = iota
	OpEQ
	OpGT
	OpLE
	OpLT
	OpGE
	OpMATCH // "MATCH" operator (e.g. for FTS or KNN semantics)
	OpNE
	OpIS
	OpISNOT
	OpISNULL
	OpISNOTNULL
	OpLIKE
	OpGLOB
	OpREGEXP
	OpFUNCTION
	OpLIMIT
	OpOFFSET
)

// Constraint describes a single WHERE-clause constraint that SQLite is
// considering pushing down to the virtual table.
type Constraint struct {
	Column int
	Op     ConstraintOp
	Usable bool
	// ArgIndex selects which position in argv[] (0-based) should contain the
	// RHS value for this constraint when Filter is called. Set to -1 to ignore.
	ArgIndex int
	// Omit requests SQLite to omit the corresponding constraint from the
	// parent query if the virtual table fully handles it.
	Omit bool
	// IsIn indicates whether this constraint represents an IN(...) expression.
	// When true, the corresponding Value in Filter can be iterated using
	// Value.InFirst() and Value.InNext() to access each element.
	IsIn bool
}

// OrderBy describes a single ORDER BY term for a query involving a virtual
// table.
type OrderBy struct {
	Column int
	Desc   bool
}

// IndexInfo holds information about constraints and orderings for a virtual
// table query. It is the Go analogue of sqlite3_index_info.
type IndexInfo struct {
	Constraints []Constraint
	OrderBy     []OrderBy

	// IdxNum selects the query plan chosen in BestIndex. This value is passed
	// back to Cursor.Filter. Note: SQLite stores this as a 32-bit signed
	// integer (int32). Implementations must ensure IdxNum fits within the
	// int32 range; values outside of int32 will cause an error in the driver
	// to avoid silent truncation.
	IdxNum int64
	IdxStr string
	// IdxFlags provides extra information about the chosen plan.
	// Set to IndexScanUnique to indicate the plan visits at most one row.
	IdxFlags        int
	OrderByConsumed bool
	EstimatedCost   float64
	EstimatedRows   int64
	// ColUsed is a bitmask indicating which columns are used by the query.
	// Bit N is set if column N is referenced.
	ColUsed uint64

	// Internal fields used by the engine to support IN constraint handling.
	// Modules should not access these directly.
	_isIn     func(iCons int) bool        // Check if constraint is IN
	_handleIn func(iCons int, handle int) // Declare handling of IN
}

// IsInConstraint returns true if the constraint at the given index is an
// IN(...) expression. This is useful in BestIndex to optimize IN queries.
func (ii *IndexInfo) IsInConstraint(iCons int) bool {
	if ii._isIn == nil || iCons < 0 || iCons >= len(ii.Constraints) {
		return false
	}
	return ii._isIn(iCons)
}

// HandleInConstraint tells SQLite that the virtual table will handle the
// IN constraint at the given index. Set handle=true to declare handling,
// or false to let SQLite handle it via the normal evaluation path.
func (ii *IndexInfo) HandleInConstraint(iCons int, handle bool) {
	if ii._handleIn == nil || iCons < 0 || iCons >= len(ii.Constraints) {
		return
	}
	h := 0
	if handle {
		h = 1
	}
	ii._handleIn(iCons, h)
}

// SetIsInFunc sets the internal function for checking IN constraints.
// This is used by the engine and should not be called by modules.
func (ii *IndexInfo) SetIsInFunc(fn func(iCons int) bool) {
	ii._isIn = fn
}

// SetHandleInFunc sets the internal function for handling IN constraints.
// This is used by the engine and should not be called by modules.
func (ii *IndexInfo) SetHandleInFunc(fn func(iCons int, handle int)) {
	ii._handleIn = fn
}

// Index flag values for IndexInfo.IdxFlags.
const (
	// IndexScanUnique mirrors SQLITE_INDEX_SCAN_UNIQUE and indicates that the
	// chosen plan will visit at most one row.
	IndexScanUnique = 1
)

// ErrNotImplemented is returned by RegisterModule when the underlying engine
// has not yet installed a registration hook. External projects can depend on
// the vtab API surface before the low-level bridge to sqlite3_create_module
// is fully wired; once the engine sets the hook via SetRegisterFunc,
// RegisterModule will forward calls to it.
var ErrNotImplemented = errors.New("vtab: RegisterModule not wired into engine")

// registerHook is installed by the engine package via SetRegisterFunc. It is
// invoked by RegisterModule to perform the actual module registration.
var registerHook func(name string, m Module) error

// SetRegisterFunc is intended to be called by the engine package to provide
// the concrete implementation of module registration. External callers
// should use RegisterModule instead.
func SetRegisterFunc(fn func(name string, m Module) error) { registerHook = fn }

// RegisterModule registers a virtual table module with the provided *sql.DB.
//
// Registration applies to new connections only. Existing open connections will
// not be updated to include newly registered modules.
//
// Registration is performed when opening a new connection. If the underlying
// sqlite3_create_module_v2 call fails, opening the connection fails and returns
// that error. This fail-fast behavior prevents partially-initialized
// connections when a module cannot be installed.
//
// The db parameter is currently unused by the engine; it is available so
// module implementations can capture it if they need a *sql.DB for their own
// internal queries.
func RegisterModule(db *sql.DB, name string, m Module) error {
	_ = db
	if registerHook == nil {
		return ErrNotImplemented
	}
	if name == "" {
		return errors.New("vtab: module name must be non-empty")
	}
	if m == nil {
		return errors.New("vtab: module implementation is nil")
	}
	return registerHook(name, m)
}

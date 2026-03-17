// Package vtab defines a Go-facing API for implementing SQLite virtual table
// modules on top of the modernc.org/sqlite driver.
//
// It is intentionally small and generic so that external projects can
// implement virtual tables without depending on the translated C internals.
package vtab

// API notes
//
// - Schema declaration: Modules must call Context.Declare from within Create
//   or Connect to declare the virtual table schema (a CREATE TABLE statement).
//   The driver no longer auto-declares based on USING(...) args to support
//   dynamic schemas (e.g., CSV headers).
//
// - Constraint support: Modules that want MATCH/other constraints must call
//   Context.EnableConstraintSupport from within Create or Connect.
//
// - Vtab config: Modules can call Context.Config to pass sqlite3_vtab_config
//   options (e.g., INNOCUOUS, DIRECTONLY) from within Create or Connect.
//
// - Constraint operators: ConstraintOp includes OpUnknown for operators that
//   are not recognized. The driver maps common SQLite operators including EQ,
//   NE, GT, GE, LT, LE, MATCH, IS/ISNOT, ISNULL/ISNOTNULL, LIKE, GLOB, REGEXP,
//   FUNCTION, LIMIT, and OFFSET.
//
// - ArgIndex: Set as 0-based in Go to indicate which position in argv[] should
//   receive a constraint value. The driver adds +1 when communicating with
//   SQLite (which is 1-based). Use -1 (default) to ignore.
//
// - Omit: Set Constraint.Omit to ask SQLite to not re-evaluate that constraint
//   in the parent query if the virtual table fully handles it.
//
// - ColUsed: IndexInfo.ColUsed provides a bitmask of columns referenced by the
//   query. Bit N indicates column N is used.
//
// - IdxFlags: IndexInfo.IdxFlags allows modules to set planning flags.
//   Currently, IndexScanUnique (mirrors SQLITE_INDEX_SCAN_UNIQUE) indicates
//   the plan will visit at most one row. This can help the optimizer.
//
// Optional interfaces
//
// - Updater: Implement on your Table to support writes via xUpdate.
//   Insert(cols, *rowid), Update(oldRowid, cols, *newRowid), Delete(oldRowid).
//   The trampoline maps SQLite xUpdate’s calling convention to these methods.
//
// - Renamer: Implement Rename(newName string) on your Table to handle xRename.
//   If unimplemented, rename is treated as a no-op.
//
// - Transactional: Implement Begin/Sync/Commit/Rollback/Savepoint/Release/
//   RollbackTo as needed. Unimplemented methods are treated as no-ops.
//   These callbacks let writable or advanced modules coordinate with SQLite
//   transaction boundaries.
//
// Re-entrancy cautions
// - Avoid executing SQL on the same connection from within vtab methods
//   (Create/Connect/BestIndex/Filter/etc.). SQLite virtual table callbacks run
//   inside the engine and issuing re-entrant SQL on the same connection can
//   lead to deadlocks or undefined behavior. If a module requires issuing SQL,
//   consider using a separate connection and document the concurrency model.
//
// Complete Example
//
// This example demonstrates how to implement a simple key-value virtual table:
//
//	package main
//
//	import (
//		"database/sql"
//		"fmt"
//		"sync"
//
//		"modernc.org/sqlite"
//		_ "modernc.org/sqlite"
//		"modernc.org/sqlite/vtab"
//	)
//
//	// KVModule implements vtab.Module for a simple key-value table
//	type KVModule struct {
//		mu     sync.RWMutex
//		tables map[string]*KVTable
//	}
//
//	func NewKVModule() *KVModule {
//		return &KVModule{tables: make(map[string]*KVTable)}
//	}
//
//	func (m *KVModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
//		// args[0] = module name, args[1] = db name, args[2] = table name, args[3...] = module args
//		tableName := args[2]
//		return m.connect(ctx, tableName)
//	}
//
//	func (m *KVModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
//		tableName := args[2]
//		return m.connect(ctx, tableName)
//	}
//
//	func (m *KVModule) connect(ctx vtab.Context, name string) (vtab.Table, error) {
//		// Declare the virtual table schema
//		if err := ctx.Declare("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT)"); err != nil {
//			return nil, err
//		}
//
//		table := &KVTable{name: name, data: make(map[string]string)}
//		m.mu.Lock()
//		m.tables[name] = table
//		m.mu.Unlock()
//		return table, nil
//	}
//
//	// KVTable implements vtab.Table with read/write support
//	type KVTable struct {
//		name string
//		mu   sync.RWMutex
//		data map[string]string
//	}
//
//	func (t *KVTable) BestIndex(info *vtab.IndexInfo) error {
//		// For simplicity, always use full scan
//		info.EstimatedCost = 1000000
//		info.OrderByConsumed = false
//		return nil
//	}
//
//	func (t *KVTable) Open() (vtab.Cursor, error) {
//		return &KVCursor{table: t}, nil
//	}
//
//	func (t *KVTable) Disconnect() error { return nil }
//	func (t *KVTable) Destroy() error    { return nil }
//
//	// Implement UpdaterWithContext for write support
//	func (t *KVTable) InsertWithContext(ctx vtab.Context, cols []vtab.Value, rowid *int64) error {
//		if len(cols) < 2 {
//			return fmt.Errorf("insert requires key and value")
//		}
//		key, _ := cols[0].(string)
//		value, _ := cols[1].(string)
//		t.mu.Lock()
//		t.data[key] = value
//		t.mu.Unlock()
//		return nil
//	}
//
//	func (t *KVTable) UpdateWithContext(ctx vtab.Context, oldRowid int64, cols []vtab.Value, newRowid *int64) error {
//		if len(cols) < 2 {
//			return fmt.Errorf("update requires key and value")
//		}
//		key, _ := cols[0].(string)
//		value, _ := cols[1].(string)
//		t.mu.Lock()
//		t.data[key] = value
//		t.mu.Unlock()
//		return nil
//	}
//
//	func (t *KVTable) DeleteWithContext(ctx vtab.Context, rowid int64) error {
//		return nil
//	}
//
//	// KVCursor implements vtab.Cursor for scanning
//	type KVCursor struct {
//		table *KVTable
//		keys  []string
//		idx   int
//	}
//
//	func (c *KVCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
//		c.table.mu.RLock()
//		defer c.table.mu.RUnlock()
//		c.keys = make([]string, 0, len(c.table.data))
//		for k := range c.table.data {
//			c.keys = append(c.keys, k)
//		}
//		c.idx = 0
//		return nil
//	}
//
//	func (c *KVCursor) Next() error {
//		if c.idx < len(c.keys) {
//			c.idx++
//		}
//		return nil
//	}
//
//	func (c *KVCursor) Eof() bool {
//		return c.idx >= len(c.keys)
//	}
//
//	func (c *KVCursor) Column(col int) (vtab.Value, error) {
//		if c.idx >= len(c.keys) {
//			return nil, nil
//		}
//		key := c.keys[c.idx]
//		c.table.mu.RLock()
//		value := c.table.data[key]
//		c.table.mu.RUnlock()
//		if col == 0 {
//			return key, nil
//		}
//		return value, nil
//	}
//
//	func (c *KVCursor) Rowid() (int64, error) {
//		return int64(c.idx + 1), nil
//	}
//
//	func (c *KVCursor) Close() error { return nil }
//
//	func main() {
//		sql.Register("kvdriver", &sqlite.Driver{})
//		db, _ := sql.Open("kvdriver", ":memory:")
//		defer db.Close()
//
//		module := NewKVModule()
//		if err := vtab.RegisterModule(db, "kv", module); err != nil {
//			panic(err)
//		}
//
//		db.Exec("CREATE VIRTUAL TABLE mydata USING kv()")
//		db.Exec("INSERT INTO mydata VALUES (?, ?)", "name", "test")
//
//		rows, _ := db.Query("SELECT * FROM mydata")
//		for rows.Next() {
//			var k, v string
//			rows.Scan(&k, &v)
//			fmt.Println(k, v)
//		}
//	}
//
// Best Practices
//
// 1. Shadow Tables for Persistence
//
// Virtual tables should use shadow tables for data persistence. Create shadow
// tables before CREATE VIRTUAL TABLE, then use ctx.Exec() or ctx.OpenBlob()
// within transaction callbacks to sync data.
//
// 2. Thread Safety
//
//   - Use sync.RWMutex for read-heavy workloads
//   - Consider connection pooling for concurrent access
//   - Be aware that vtab callbacks may be called concurrently from multiple
//     goroutines when using WAL mode with busy_timeout.
//
// 3. Memory Management
//
//   - Release resources in Cursor.Close()
//   - Clear large caches in Table.Destroy()
//   - Use value pooling for high-frequency allocations
//
// 4. Error Handling
//
//   - Always return descriptive errors from vtab methods
//   - Use setVtabZErrMsg to set error messages for SQLite
//   - Propagate errors from ctx.Exec() and ctx.OpenBlob()
//
// 5. BestIndex Optimization
//
//   - Set EstimatedCost accurately based on row count
//   - Use Constraint.Omit for fully handled constraints
//   - Set IdxFlags = IndexScanUnique for point queries
//   - Leverage idxStr to encode complex query plans
//
// 6. Transaction Coordination
//
//   - Implement TransactionalWithContext for ACID guarantees
//   - Use pending operations queue, commit in Sync/Commit
//   - Implement Rollback to discard pending changes

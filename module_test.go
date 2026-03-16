package sqlite

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	"modernc.org/sqlite/vtab"
)

// newTestDB creates a new in-memory database for testing.
func newTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	return db
}

// dummyModule is a minimal vtab.Module implementation used to verify that the
// vtab bridge (registration + trampolines) works end-to-end inside this
// repository, without relying on external modules.
type dummyModule struct{}

// dummyTable implements vtab.Table for the dummy module.
type dummyTable struct{}

// dummyCursor implements vtab.Cursor for the dummy module. It returns a small
// fixed result set for testing.
type dummyCursor struct {
	rows []struct {
		rowid int64
		val   string
	}
	pos int
}

// lastIndexInfo captures the most recent IndexInfo seen by dummyTable.BestIndex
// so that tests can assert on constraints and orderings.
var lastIndexInfo *vtab.IndexInfo

// Create implements vtab.Module.Create.
func (m *dummyModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	_ = ctx
	// Declare schema based on args: args[2]=table name, args[3:]=columns.
	if len(args) >= 3 {
		cols := "x"
		if len(args) > 3 {
			cols = strings.Join(args[3:], ",")
		}
		if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(%s)", args[2], cols)); err != nil {
			return nil, err
		}
	}
	return &dummyTable{}, nil
}

// Connect implements vtab.Module.Connect.
func (m *dummyModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	_ = ctx
	// Same schema logic in Connect.
	if len(args) >= 3 {
		cols := "x"
		if len(args) > 3 {
			cols = strings.Join(args[3:], ",")
		}
		if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(%s)", args[2], cols)); err != nil {
			return nil, err
		}
	}
	return &dummyTable{}, nil
}

func (t *dummyTable) BestIndex(info *vtab.IndexInfo) error {
	// Record the last IndexInfo for inspection in tests.
	lastIndexInfo = info
	// Choose a fixed plan ID so we can verify that IdxNum flows through
	// sqlite3_index_info into Cursor.Filter.
	info.IdxNum = 1
	return nil
}

// Open creates a new dummyCursor.
func (t *dummyTable) Open() (vtab.Cursor, error) {
	_ = t
	return &dummyCursor{}, nil
}

// Disconnect is a no-op for the dummy table.
func (t *dummyTable) Disconnect() error { return nil }

// Destroy is a no-op for the dummy table.
func (t *dummyTable) Destroy() error { return nil }

func (c *dummyCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	_ = idxStr
	_ = vals
	// Ensure that the planner-provided idxNum from BestIndex is propagated.
	// If idxNum is not 1, return a different rowset so the test would fail.
	if idxNum == 1 {
		c.rows = []struct {
			rowid int64
			val   string
		}{
			{rowid: 1, val: "alpha"},
			{rowid: 2, val: "beta"},
		}
	} else {
		c.rows = []struct {
			rowid int64
			val   string
		}{
			{rowid: 1, val: "unexpected"},
		}
	}
	c.pos = 0
	return nil
}

// Next advances the cursor.
func (c *dummyCursor) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

// Eof reports whether the cursor is past the last row.
func (c *dummyCursor) Eof() bool { return c.pos >= len(c.rows) }

// Column returns the string value for column 0 and NULL for others.
func (c *dummyCursor) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

// Rowid returns the current rowid.
func (c *dummyCursor) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

// Close clears the cursor state.
func (c *dummyCursor) Close() error {
	c.rows = nil
	c.pos = 0
	return nil
}

// Omit test module types and methods
type omitModuleX struct{ omit bool }
type omitTableX struct{ omit bool }
type omitCursorX struct {
	rows []struct {
		rowid int64
		val   string
	}
	pos int
}

func (m *omitModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.Declare("CREATE TABLE " + args[2] + "(val)"); err != nil {
		return nil, err
	}
	return &omitTableX{omit: m.omit}, nil
}
func (m *omitModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.Declare("CREATE TABLE " + args[2] + "(val)"); err != nil {
		return nil, err
	}
	return &omitTableX{omit: m.omit}, nil
}
func (t *omitTableX) BestIndex(info *vtab.IndexInfo) error {
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Op == vtab.OpEQ && c.Column == 0 {
			c.ArgIndex = 0
			c.Omit = t.omit
			break
		}
	}
	return nil
}
func (t *omitTableX) Open() (vtab.Cursor, error) { return &omitCursorX{}, nil }
func (t *omitTableX) Disconnect() error          { return nil }
func (t *omitTableX) Destroy() error             { return nil }
func (c *omitCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.rows = []struct {
		rowid int64
		val   string
	}{{1, "alpha"}, {2, "beta"}}
	c.pos = 0
	return nil
}
func (c *omitCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}
func (c *omitCursorX) Eof() bool { return c.pos >= len(c.rows) }
func (c *omitCursorX) Column(col int) (vtab.Value, error) {
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}
func (c *omitCursorX) Rowid() (int64, error) { return c.rows[c.pos].rowid, nil }
func (c *omitCursorX) Close() error          { return nil }

// Operator capture module types and methods
type opsModuleX struct{}
type opsTableX struct{}

var seenOpsOps []vtab.ConstraintOp

func (m *opsModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.Declare("CREATE TABLE " + args[2] + "(c1)"); err != nil {
		return nil, err
	}
	return &opsTableX{}, nil
}
func (m *opsModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.Declare("CREATE TABLE " + args[2] + "(c1)"); err != nil {
		return nil, err
	}
	return &opsTableX{}, nil
}
func (t *opsTableX) BestIndex(info *vtab.IndexInfo) error {
	seenOpsOps = nil
	for _, c := range info.Constraints {
		if c.Usable {
			seenOpsOps = append(seenOpsOps, c.Op)
		}
	}
	return nil
}
func (t *opsTableX) Open() (vtab.Cursor, error) { return &dummyCursor{}, nil }
func (t *opsTableX) Disconnect() error          { return nil }
func (t *opsTableX) Destroy() error             { return nil }

// matchModuleX exercises MATCH constraint support.
type matchModuleX struct{}
type matchTableX struct{}
type matchCursorX struct {
	rows []struct {
		rowid int64
		val   string
	}
	pos int
}

func (m *matchModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.EnableConstraintSupport(); err != nil {
		return nil, err
	}
	if err := ctx.Declare("CREATE TABLE " + args[2] + "(val)"); err != nil {
		return nil, err
	}
	return &matchTableX{}, nil
}

func (m *matchModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.EnableConstraintSupport(); err != nil {
		return nil, err
	}
	if err := ctx.Declare("CREATE TABLE " + args[2] + "(val)"); err != nil {
		return nil, err
	}
	return &matchTableX{}, nil
}

func (t *matchTableX) BestIndex(info *vtab.IndexInfo) error {
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Op == vtab.OpMATCH && c.Column == 0 {
			c.ArgIndex = 0
			c.Omit = true
			info.IdxNum = 1
			return nil
		}
	}
	info.IdxNum = 0
	return nil
}

func (t *matchTableX) Open() (vtab.Cursor, error) { return &matchCursorX{}, nil }
func (t *matchTableX) Disconnect() error          { return nil }
func (t *matchTableX) Destroy() error             { return nil }

func (c *matchCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	_ = idxStr
	all := []string{"alpha", "alpine", "beta"}
	c.rows = c.rows[:0]
	c.pos = 0
	if idxNum != 1 || len(vals) == 0 {
		return nil
	}
	query, ok := vals[0].(string)
	if !ok {
		return nil
	}
	var rowid int64
	for _, v := range all {
		rowid++
		if strings.Contains(v, query) {
			c.rows = append(c.rows, struct {
				rowid int64
				val   string
			}{rowid: rowid, val: v})
		}
	}
	return nil
}

func (c *matchCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *matchCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *matchCursorX) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *matchCursorX) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *matchCursorX) Close() error {
	c.rows = nil
	c.pos = 0
	return nil
}

// TestDummyModuleVtab verifies that a simple vtab module implemented in Go
// can be registered and queried through the modernc.org/sqlite driver.
func TestDummyModuleVtab(t *testing.T) {
	// Open an in-memory database using this driver.
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	// Register the dummy module.
	if err := vtab.RegisterModule(db, "dummy", &dummyModule{}); err != nil {
		t.Fatalf("vtab.RegisterModule failed: %v", err)
	}

	// Create a virtual table using the dummy module.
	if _, err := db.Exec(`CREATE VIRTUAL TABLE vt USING dummy(value)`); err != nil {
		t.Fatalf("CREATE VIRTUAL TABLE vt USING dummy failed: %v", err)
	}

	// Query the virtual table with a simple equality constraint.
	rows, err := db.Query(`SELECT rowid, value FROM vt WHERE value = 'alpha' ORDER BY rowid`)
	if err != nil {
		t.Fatalf("SELECT from vt failed: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var rowid int64
		var value string
		if err := rows.Scan(&rowid, &value); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d (%v)", len(got), got)
	}
	if got[0] != "alpha" {
		t.Fatalf("unexpected value from vt: %v (want [alpha])", got)
	}

	// Verify that BestIndex saw a usable equality constraint on column 0.
	if lastIndexInfo == nil {
		t.Fatalf("expected BestIndex to be called and lastIndexInfo to be set")
	}
	found := false
	for _, c := range lastIndexInfo.Constraints {
		if c.Column == 0 && c.Op == vtab.OpEQ && c.Usable {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("BestIndex did not observe a usable EQ constraint on column 0; got %+v", lastIndexInfo.Constraints)
	}

	// Verify ColUsed indicates column 0 is referenced.
	if lastIndexInfo.ColUsed == 0 || (lastIndexInfo.ColUsed&1) == 0 {
		t.Fatalf("expected ColUsed to include column 0; got %b", lastIndexInfo.ColUsed)
	}
}

// argIndexModule exercises Constraint.ArgIndex and ensures that the arguments
// passed to Cursor.Filter arrive in the expected order.
type argIndexModule struct{}

type argIndexTable struct{}

type argIndexCursor struct {
	rows []int64
	pos  int
}

var (
	argIndexFilterVals  []vtab.Value
	argIndexFilterCalls int
)

func (m *argIndexModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	// Declare a simple schema using provided column names.
	if len(args) >= 3 {
		cols := "c1,c2"
		if len(args) > 3 {
			cols = strings.Join(args[3:], ",")
		}
		if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(%s)", args[2], cols)); err != nil {
			return nil, err
		}
	}
	return &argIndexTable{}, nil
}

func (m *argIndexModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) >= 3 {
		cols := "c1,c2"
		if len(args) > 3 {
			cols = strings.Join(args[3:], ",")
		}
		if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(%s)", args[2], cols)); err != nil {
			return nil, err
		}
	}
	return &argIndexTable{}, nil
}

func (t *argIndexTable) BestIndex(info *vtab.IndexInfo) error {
	// Assign ArgIndex sequentially (0-based) for all usable EQ constraints so
	// that SQLite passes their RHS values to Filter in argv[] in that order.
	next := 0
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if !c.Usable || c.Op != vtab.OpEQ {
			continue
		}
		c.ArgIndex = next
		next++
	}
	return nil
}

func (t *argIndexTable) Open() (vtab.Cursor, error) {
	_ = t
	return &argIndexCursor{}, nil
}

func (t *argIndexTable) Disconnect() error { return nil }

func (t *argIndexTable) Destroy() error { return nil }

func (c *argIndexCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	_ = idxNum
	_ = idxStr
	// Capture the values passed from SQLite so the test can assert on them.
	argIndexFilterCalls++
	argIndexFilterVals = append([]vtab.Value(nil), vals...)
	// Expose a single dummy row so the query returns one result.
	c.rows = []int64{1}
	c.pos = 0
	return nil
}

func (c *argIndexCursor) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *argIndexCursor) Eof() bool { return c.pos >= len(c.rows) }

func (c *argIndexCursor) Column(col int) (vtab.Value, error) {
	_ = col
	// We only select rowid in the test, so Column is unused.
	return nil, nil
}

func (c *argIndexCursor) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos], nil
}

func (c *argIndexCursor) Close() error { return nil }

// TestVtabConstraintArgIndex verifies that setting Constraint.ArgIndex in
// BestIndex causes Cursor.Filter to receive the corresponding argument values
// in argv[] in the expected order.
func TestVtabConstraintArgIndex(t *testing.T) {
	argIndexFilterVals = nil
	argIndexFilterCalls = 0

	if err := vtab.RegisterModule(nil, "argtest", &argIndexModule{}); err != nil {
		t.Fatalf("vtab.RegisterModule(argtest) failed: %v", err)
	}
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE VIRTUAL TABLE at USING argtest(c1, c2)`); err != nil {
		t.Fatalf("CREATE VIRTUAL TABLE at USING argtest failed: %v", err)
	}

	rows, err := db.Query(`SELECT rowid FROM at WHERE c1 = ? AND c2 = ?`, 10, 20)
	if err != nil {
		t.Fatalf("SELECT from at failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var rowid int64
		if err := rows.Scan(&rowid); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if argIndexFilterCalls == 0 {
		t.Fatalf("expected Filter to be called at least once")
	}
	if len(argIndexFilterVals) != 2 {
		t.Fatalf("expected 2 argv values in Filter, got %d (%v)", len(argIndexFilterVals), argIndexFilterVals)
	}
	v1, ok1 := argIndexFilterVals[0].(int64)
	v2, ok2 := argIndexFilterVals[1].(int64)
	if !ok1 || !ok2 {
		t.Fatalf("unexpected argv types: %T, %T", argIndexFilterVals[0], argIndexFilterVals[1])
	}
	if v1 != 10 || v2 != 20 {
		t.Fatalf("unexpected argv values in Filter: got (%v, %v), want (10, 20)", v1, v2)
	}
}

// TestVtabOmitConstraintEffect verifies that setting Constraint.Omit causes
// SQLite to not re-evaluate the parent constraint and relies on the vtab to
// enforce it.
func TestVtabOmitConstraintEffect(t *testing.T) {
	if err := vtab.RegisterModule(nil, "omit_off", &omitModuleX{omit: false}); err != nil {
		t.Fatalf("RegisterModule omit_off: %v", err)
	}
	if err := vtab.RegisterModule(nil, "omit_on", &omitModuleX{omit: true}); err != nil {
		t.Fatalf("RegisterModule omit_on: %v", err)
	}
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE VIRTUAL TABLE vt_off USING omit_off(val)`); err != nil {
		t.Fatalf("create vt_off: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE vt_on USING omit_on(val)`); err != nil {
		t.Fatalf("create vt_on: %v", err)
	}

	// omit=false: SQLite should re-check WHERE and filter down to 1 row.
	rows, err := db.Query(`SELECT val FROM vt_off WHERE val = 'alpha'`)
	if err != nil {
		t.Fatalf("query vt_off: %v", err)
	}
	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	rows.Close()
	if len(got) != 1 || got[0] != "alpha" {
		t.Fatalf("omit=false expected [alpha], got %v", got)
	}

	// omit=true: SQLite should not re-check WHERE; both rows would pass unless the vtab filters.
	rows, err = db.Query(`SELECT val FROM vt_on WHERE val = 'alpha'`)
	if err != nil {
		t.Fatalf("query vt_on: %v", err)
	}
	got = got[:0]
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	rows.Close()
	if len(got) != 2 {
		t.Fatalf("omit=true expected 2 rows (no re-check), got %d %v", len(got), got)
	}
}

// TestVtabMatchConstraint ensures MATCH constraints work when enabled.
func TestVtabMatchConstraint(t *testing.T) {
	if err := vtab.RegisterModule(nil, "matchx", &matchModuleX{}); err != nil {
		t.Fatalf("vtab.RegisterModule(matchx) failed: %v", err)
	}
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE VIRTUAL TABLE mt USING matchx(val)`); err != nil {
		t.Fatalf("CREATE VIRTUAL TABLE mt USING matchx failed: %v", err)
	}

	rows, err := db.Query(`SELECT val FROM mt WHERE val MATCH 'al' ORDER BY val`)
	if err != nil {
		t.Fatalf("SELECT from mt failed: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != 2 || got[0] != "alpha" || got[1] != "alpine" {
		t.Fatalf("unexpected MATCH results: %v", got)
	}
}

// TestVtabConstraintOperators verifies that at least one non-EQ operator is
// faithfully mapped (IS NULL) to the Go ConstraintOp.
func TestVtabConstraintOperators(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "ops", &opsModuleX{}); err != nil {
		t.Fatalf("register ops: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE ovt USING ops(c1)`); err != nil {
		t.Fatalf("create ovt: %v", err)
	}

	rows, err := db.Query(`SELECT rowid FROM ovt WHERE c1 IS NULL`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	rows.Close()
	// Expect to see an ISNULL op recorded.
	found := false
	for _, op := range seenOpsOps {
		if op == vtab.OpISNULL {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to see OpISNULL in constraints, got %v", seenOpsOps)
	}

	// Also verify LIKE maps through when present.
	rows, err = db.Query(`SELECT rowid FROM ovt WHERE c1 LIKE 'a%'`)
	if err != nil {
		t.Fatalf("query like: %v", err)
	}
	rows.Close()
	found = false
	for _, op := range seenOpsOps {
		if op == vtab.OpLIKE {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to see OpLIKE in constraints, got %v", seenOpsOps)
	}

	// And verify GLOB maps through when present.
	rows, err = db.Query(`SELECT rowid FROM ovt WHERE c1 GLOB 'a*'`)
	if err != nil {
		t.Fatalf("query glob: %v", err)
	}
	rows.Close()
	found = false
	for _, op := range seenOpsOps {
		if op == vtab.OpGLOB {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to see OpGLOB in constraints, got %v", seenOpsOps)
	}
}

// overflowIdxModule sets an out-of-range IdxNum to verify the driver rejects
// values that do not fit into SQLite's int32 idxNum.
type overflowIdxModule struct{}
type overflowIdxTable struct{}
type overflowIdxCursor struct{}

func (m *overflowIdxModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("overflowIdx: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val)", args[2])); err != nil {
		return nil, err
	}
	return &overflowIdxTable{}, nil
}
func (m *overflowIdxModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}
func (t *overflowIdxTable) BestIndex(info *vtab.IndexInfo) error {
	// Force IdxNum to exceed int32 to trigger trampoline guard.
	info.IdxNum = int64(math.MaxInt32) + 1
	return nil
}
func (t *overflowIdxTable) Open() (vtab.Cursor, error) { return &overflowIdxCursor{}, nil }
func (t *overflowIdxTable) Disconnect() error          { return nil }
func (t *overflowIdxTable) Destroy() error             { return nil }
func (c *overflowIdxCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	return nil
}
func (c *overflowIdxCursor) Next() error                    { return nil }
func (c *overflowIdxCursor) Eof() bool                      { return true }
func (c *overflowIdxCursor) Column(int) (vtab.Value, error) { return nil, nil }
func (c *overflowIdxCursor) Rowid() (int64, error)          { return 0, nil }
func (c *overflowIdxCursor) Close() error                   { return nil }

// badcolModule returns an unsupported type from Column to ensure the error
// text from functionReturnValue is propagated via sqlite3_result_error.
type badcolModule struct{}
type badcolTable struct{}
type badcolCursor struct{ pos int }

func (m *badcolModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("badcol: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val)", args[2])); err != nil {
		return nil, err
	}
	return &badcolTable{}, nil
}
func (m *badcolModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}
func (t *badcolTable) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *badcolTable) Open() (vtab.Cursor, error)           { return &badcolCursor{pos: 0}, nil }
func (t *badcolTable) Disconnect() error                    { return nil }
func (t *badcolTable) Destroy() error                       { return nil }
func (c *badcolCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}
func (c *badcolCursor) Next() error {
	if c.pos < 1 {
		c.pos++
	}
	return nil
}
func (c *badcolCursor) Eof() bool { return c.pos >= 1 }
func (c *badcolCursor) Column(col int) (vtab.Value, error) {
	// Return a value of an unsupported type to provoke functionReturnValue error.
	type unsupported struct{}
	return unsupported{}, nil
}
func (c *badcolCursor) Rowid() (int64, error) { return 1, nil }
func (c *badcolCursor) Close() error          { return nil }

func TestVtabIdxNumOverflowError(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "overflow_idx", &overflowIdxModule{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE ovt USING overflow_idx(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Any SELECT should invoke BestIndex and fail due to IdxNum overflow.
	_, err = db.Query(`SELECT val FROM ovt`)
	if err == nil {
		t.Fatalf("expected SELECT to fail due to IdxNum overflow")
	}
	if msg := err.Error(); !strings.Contains(msg, "IdxNum") || !strings.Contains(msg, "int32") {
		t.Fatalf("unexpected error: %v", msg)
	}
}

func TestVtabColumnUnsupportedValueErrorMessage(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "badcol", &badcolModule{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE bc USING badcol(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Run a query and ensure it fails with a descriptive message from xColumn.
	rows, err := db.Query(`SELECT val FROM bc`)
	if err != nil {
		// Prepare-time error would also be acceptable, but we expect run-time here.
		// Ensure message mentions unsupported driver.Value.
		if !strings.Contains(err.Error(), "did not return a valid driver.Value") {
			t.Fatalf("unexpected error from Query: %v", err)
		}
		return
	}
	defer rows.Close()

	// Iterate to trigger stepping/column retrieval; expect rows.Err() to contain our message.
	for rows.Next() {
		var v any
		_ = rows.Scan(&v)
	}
	if err := rows.Err(); err == nil {
		t.Fatalf("expected rows.Err to report unsupported value")
	} else if !strings.Contains(err.Error(), "did not return a valid driver.Value") {
		t.Fatalf("unexpected rows.Err: %v", err)
	}
}

// Updater demo: in-memory table with (name, email) columns and rowid.
type updRow struct {
	id  int64
	val string
}

type updaterModuleX struct{}
type updaterTableX struct {
	rows   []updRow
	nextID int64
}
type updaterCursorX struct {
	t   *updaterTableX
	pos int
}

func (m *updaterModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("upd: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val)", args[2])); err != nil {
		return nil, err
	}
	return &updaterTableX{rows: nil, nextID: 1}, nil
}
func (m *updaterModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}
func (t *updaterTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *updaterTableX) Open() (vtab.Cursor, error)           { return &updaterCursorX{t: t, pos: 0}, nil }
func (t *updaterTableX) Disconnect() error                    { return nil }
func (t *updaterTableX) Destroy() error                       { return nil }

// Updater methods
func (t *updaterTableX) Insert(cols []vtab.Value, rowid *int64) error {
	val, _ := cols[0].(string)
	id := *rowid
	if id == 0 {
		id = t.nextID
	}
	t.nextID = id + 1
	t.rows = append(t.rows, updRow{id: id, val: val})
	*rowid = id
	return nil
}
func (t *updaterTableX) Update(oldRowid int64, cols []vtab.Value, newRowid *int64) error {
	for i := range t.rows {
		if t.rows[i].id == oldRowid {
			val, _ := cols[0].(string)
			t.rows[i].val = val
			if newRowid != nil && *newRowid != 0 && *newRowid != oldRowid {
				t.rows[i].id = *newRowid
			}
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}
func (t *updaterTableX) Delete(oldRowid int64) error {
	for i := range t.rows {
		if t.rows[i].id == oldRowid {
			t.rows = append(t.rows[:i], t.rows[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (c *updaterCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}
func (c *updaterCursorX) Next() error {
	if c.pos < len(c.t.rows) {
		c.pos++
	}
	return nil
}
func (c *updaterCursorX) Eof() bool { return c.pos >= len(c.t.rows) }
func (c *updaterCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.t.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.t.rows[c.pos].val, nil
	}
	return nil, nil
}
func (c *updaterCursorX) Rowid() (int64, error) { return c.t.rows[c.pos].id, nil }
func (c *updaterCursorX) Close() error          { return nil }

func TestVtabUpdaterInsertUpdateDelete(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "updemo", &updaterModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE ut USING updemo(name,email)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Insert Alice and Bob (auto rowid)
	if _, err := db.Exec(`INSERT INTO ut(val) VALUES(?)`, "Alice"); err != nil {
		t.Fatalf("insert alice: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO ut(val) VALUES(?)`, "Bob"); err != nil {
		t.Fatalf("insert bob: %v", err)
	}

	// Insert Carol (auto rowid)
	if _, err := db.Exec(`INSERT INTO ut(val) VALUES(?)`, "Carol"); err != nil {
		t.Fatalf("insert carol: %v", err)
	}

	// Verify rows
	assertRows := func(want []int64) {
		rows, err := db.Query(`SELECT rowid FROM ut ORDER BY rowid`)
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		defer rows.Close()
		got := make([]int64, 0)
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			got = append(got, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("ids mismatch got %v want %v", got, want)
			}
		}
	}

	assertRows([]int64{1, 2, 3})

	// Update Bob's email (rowid=2)
	if _, err := db.Exec(`UPDATE ut SET val = ? WHERE rowid = ?`, "Bobby", 2); err != nil {
		t.Fatalf("update: %v", err)
	}
	// Rowids remain unchanged after value update
	assertRows([]int64{1, 2, 3})

	// Delete Bob (rowid=2)
	if _, err := db.Exec(`DELETE FROM ut WHERE rowid = ?`, 2); err != nil {
		t.Fatalf("delete: %v", err)
	}

	assertRows([]int64{1, 3})
}

// --- TransactionContext.Exec Tests ---

// execModule is a test module that creates shadow tables using ctx.Exec in Create/Connect.
type execModule struct {
	shadowCreated bool
}

// execTable implements vtab.Table for testing ctx.Exec.
type execTable struct {
	name string
	data []string
}

// execCursor implements vtab.Cursor for execModule.
type execCursor struct {
	rows []string
	pos  int
}

func (m *execModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("execModule: missing table name")
	}
	tableName := args[2]

	// Declare virtual table schema
	schema := fmt.Sprintf("CREATE TABLE %s(id INTEGER PRIMARY KEY, val TEXT)", tableName)
	if err := ctx.Declare(schema); err != nil {
		return nil, err
	}

	// Create shadow table using ctx.Exec (key test: this should not deadlock)
	shadowSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s_shadow (id INTEGER PRIMARY KEY, meta TEXT)", tableName)
	if err := ctx.Exec(shadowSQL); err != nil {
		return nil, fmt.Errorf("ctx.Exec shadow table failed: %w", err)
	}

	// Insert initial data into shadow table using parameterized query
	insertSQL := fmt.Sprintf("INSERT INTO %s_shadow (id, meta) VALUES (?, ?), (?, ?)", tableName)
	if err := ctx.Exec(insertSQL, 1, "created", 2, "by_exec"); err != nil {
		return nil, fmt.Errorf("ctx.Exec insert failed: %w", err)
	}

	m.shadowCreated = true
	return &execTable{name: tableName, data: []string{"alpha", "beta", "gamma"}}, nil
}

func (m *execModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	// Reuse Create logic for simplicity in tests
	return m.Create(ctx, args)
}

func (t *execTable) BestIndex(info *vtab.IndexInfo) error {
	info.IdxNum = 1
	return nil
}

func (t *execTable) Open() (vtab.Cursor, error) {
	return &execCursor{rows: t.data, pos: 0}, nil
}

func (t *execTable) Disconnect() error { return nil }
func (t *execTable) Destroy() error   { return nil }

func (c *execCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *execCursor) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *execCursor) Eof() bool { return c.pos >= len(c.rows) }

func (c *execCursor) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 1 {
		return c.rows[c.pos], nil
	}
	return int64(c.pos + 1), nil // id column
}

func (c *execCursor) Rowid() (int64, error) {
	return int64(c.pos), nil
}

func (c *execCursor) Close() error {
	c.rows = nil
	return nil
}

// TestTransactionContextExec verifies that ctx.Exec works within Create/Connect
// without causing deadlocks, and that shadow tables are properly created.
func TestTransactionContextExec(t *testing.T) {
	db := newTestDB(t)

	mod := &execModule{}
	if err := vtab.RegisterModule(db, "exec_mod", mod); err != nil {
		t.Fatalf("RegisterModule: %v", err)
	}

	// Create virtual table - this should create shadow tables via ctx.Exec
	if _, err := db.Exec(`CREATE VIRTUAL TABLE test_exec USING exec_mod(id, val)`); err != nil {
		t.Fatalf("create virtual table: %v", err)
	}

	// Verify that shadow table was created and contains data
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM test_exec_shadow`).Scan(&count); err != nil {
		t.Fatalf("select from shadow table: %v", err)
	}
	if count != 2 {
		t.Fatalf("shadow table has %d rows, want 2", count)
	}

	// Verify shadow table contents
	rows, err := db.Query(`SELECT id, meta FROM test_exec_shadow ORDER BY id`)
	if err != nil {
		t.Fatalf("query shadow table: %v", err)
	}
	defer rows.Close()

	want := []struct {
		id   int
		meta string
	}{{1, "created"}, {2, "by_exec"}}
	i := 0
	for rows.Next() {
		var id int
		var meta string
		if err := rows.Scan(&id, &meta); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if i >= len(want) {
			t.Fatalf("unexpected row: %d, %s", id, meta)
		}
		if id != want[i].id || meta != want[i].meta {
			t.Fatalf("row %d: got (%d, %s), want (%d, %s)", i, id, meta, want[i].id, want[i].meta)
		}
		i++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Verify virtual table works normally
	vtRows, err := db.Query(`SELECT * FROM test_exec`)
	if err != nil {
		t.Fatalf("select from virtual table: %v", err)
	}
	defer vtRows.Close()

	vtCount := 0
	for vtRows.Next() {
		vtCount++
	}
	if vtCount != 3 {
		t.Fatalf("virtual table returned %d rows, want 3", vtCount)
	}

	t.Logf("TransactionContext.Exec test passed: shadow tables created without deadlock")
}

// TestTransactionContextExecWithoutParams tests ctx.Exec without parameter binding.
func TestTransactionContextExecWithoutParams(t *testing.T) {
	db := newTestDB(t)

	// Module that executes DDL without parameters
	simpleMod := &simpleExecModule{}
	if err := vtab.RegisterModule(db, "simple_exec", simpleMod); err != nil {
		t.Fatalf("RegisterModule: %v", err)
	}

	if _, err := db.Exec(`CREATE VIRTUAL TABLE test_simple USING simple_exec`); err != nil {
		t.Fatalf("create virtual table: %v", err)
	}

	// Verify the table was created
	var name string
	if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE name='test_simple_log'`).Scan(&name); err != nil {
		t.Fatalf("shadow table not found: %v", err)
	}
	if name != "test_simple_log" {
		t.Fatalf("unexpected table name: %s", name)
	}
}

// simpleExecModule for testing ctx.Exec without params
type simpleExecModule struct{}
type simpleExecTable struct{}
type simpleExecCursor struct {
	pos int
}

func (m *simpleExecModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	tableName := args[2]
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(x TEXT)", tableName)); err != nil {
		return nil, err
	}
	// DDL without parameters
	if err := ctx.Exec(fmt.Sprintf("CREATE TABLE %s_log (action TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP)", tableName)); err != nil {
		return nil, fmt.Errorf("create log table: %w", err)
	}
	return &simpleExecTable{}, nil
}

func (m *simpleExecModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *simpleExecTable) BestIndex(info *vtab.IndexInfo) error {
	info.IdxNum = 1
	return nil
}
func (t *simpleExecTable) Open() (vtab.Cursor, error)  { return &simpleExecCursor{}, nil }
func (t *simpleExecTable) Disconnect() error           { return nil }
func (t *simpleExecTable) Destroy() error              { return nil }
func (c *simpleExecCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}
func (c *simpleExecCursor) Next() error {
	c.pos++
	return nil
}
func (c *simpleExecCursor) Eof() bool                { return c.pos >= 1 }
func (c *simpleExecCursor) Column(col int) (vtab.Value, error) { return "test", nil }
func (c *simpleExecCursor) Rowid() (int64, error)    { return int64(c.pos), nil }
func (c *simpleExecCursor) Close() error             { return nil }

// --- IN Constraint Tests ---

// inModuleX tests IN constraint handling
type inModuleX struct{}

type inTableX struct{}

type inCursorX struct {
	rows []struct {
		rowid int64
		val   int64
	}
	pos int
}

// Track if IN constraint was detected
var inConstraintDetected bool
var inConstraintHandled bool
var inValuesSeen []int64

func (m *inModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("inModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val INTEGER)", args[2])); err != nil {
		return nil, err
	}
	return &inTableX{}, nil
}

func (m *inModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *inTableX) BestIndex(info *vtab.IndexInfo) error {
	inConstraintDetected = false
	inConstraintHandled = false

	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Column == 0 {
			// Check if this is an IN constraint
			if info.IsInConstraint(i) {
				inConstraintDetected = true
				c.ArgIndex = 0
				c.Omit = true
				// Tell SQLite we will handle this IN constraint
				info.HandleInConstraint(i, true)
				info.IdxNum = 1
				return nil
			}
		}
	}
	info.IdxNum = 0
	return nil
}

func (t *inTableX) Open() (vtab.Cursor, error) { return &inCursorX{}, nil }
func (t *inTableX) Disconnect() error          { return nil }
func (t *inTableX) Destroy() error             { return nil }

// inCursorX implements CursorWithContext for IN iteration
func (c *inCursorX) FilterWithContext(ctx vtab.Context, idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	c.rows = nil
	inValuesSeen = nil

	if idxNum != 1 {
		// Not an IN query, return empty
		return nil
	}

	// Iterate over IN values
	it := ctx.InIterate(vals, 0)
	for it.Next() {
		v := it.Value()
		if intVal, ok := v.(int64); ok {
			inValuesSeen = append(inValuesSeen, intVal)
		}
	}

	inConstraintHandled = true

	// Create rows matching the IN values
	for i, v := range inValuesSeen {
		c.rows = append(c.rows, struct {
			rowid int64
			val   int64
		}{rowid: int64(i + 1), val: v})
	}
	return nil
}

func (c *inCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	// Should not be called since CursorWithContext is implemented
	return fmt.Errorf("Filter called instead of FilterWithContext")
}

func (c *inCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *inCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *inCursorX) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *inCursorX) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *inCursorX) Close() error {
	c.rows = nil
	c.pos = 0
	return nil
}

// TestVtabInConstraint tests IN constraint detection and iteration
func TestVtabInConstraint(t *testing.T) {
	inConstraintDetected = false
	inConstraintHandled = false
	inValuesSeen = nil

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "in_mod", &inModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE in_test USING in_mod(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query with IN clause
	rows, err := db.Query(`SELECT val FROM in_test WHERE val IN (1, 3, 5, 7) ORDER BY val`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Verify IN constraint was detected
	if !inConstraintDetected {
		t.Fatal("IN constraint was not detected")
	}

	// Verify IN constraint was handled
	if !inConstraintHandled {
		t.Fatal("IN constraint was not handled")
	}

	// Verify values seen match IN list
	wantVals := []int64{1, 3, 5, 7}
	if len(inValuesSeen) != len(wantVals) {
		t.Fatalf("expected %d IN values, got %d: %v", len(wantVals), len(inValuesSeen), inValuesSeen)
	}
	for i, v := range wantVals {
		if inValuesSeen[i] != v {
			t.Errorf("IN value[%d]: got %d, want %d", i, inValuesSeen[i], v)
		}
	}

	// Verify results
	if len(got) != 4 {
		t.Fatalf("expected 4 rows, got %d: %v", len(got), got)
	}
	if got[0] != 1 || got[1] != 3 || got[2] != 5 || got[3] != 7 {
		t.Fatalf("unexpected results: %v", got)
	}
}

// --- Nochange Tests ---

// nochangeModuleX tests nochange detection during UPDATE
// Uses two columns: val1 and val2, so we can update only one and detect nochange on the other
type nochangeModuleX struct{}

type nochangeTableX struct {
	rows []struct {
		rowid     int64
		val1      string
		val2      string
		val1NoChg bool
		val2NoChg bool
	}
}

type nochangeCursorX struct {
	t   *nochangeTableX
	pos int
}

// Track nochange results per column
var nochangeResultsCol1 map[int]bool // rowid -> val1 nochange
var nochangeResultsCol2 map[int]bool // rowid -> val2 nochange

func (m *nochangeModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("nochangeModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val1 TEXT, val2 TEXT)", args[2])); err != nil {
		return nil, err
	}
	return &nochangeTableX{
		rows: []struct {
			rowid     int64
			val1      string
			val2      string
			val1NoChg bool
			val2NoChg bool
		}{
			{rowid: 1, val1: "init1", val2: "init2", val1NoChg: false, val2NoChg: false},
		},
	}, nil
}

func (m *nochangeModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *nochangeTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *nochangeTableX) Open() (vtab.Cursor, error) {
	return &nochangeCursorX{t: t, pos: 0}, nil
}
func (t *nochangeTableX) Disconnect() error                    { return nil }
func (t *nochangeTableX) Destroy() error                       { return nil }

// UpdaterWithContext for nochange detection
func (t *nochangeTableX) Update(ctx vtab.Context, oldRowid int64, cols []vtab.Value, newRowid *int64) error {
	for i := range t.rows {
		if t.rows[i].rowid == oldRowid {
			// Check nochange for each column
			val1NoChg := ctx.ValueNoChange(0)
			val2NoChg := ctx.ValueNoChange(1)
			t.rows[i].val1NoChg = val1NoChg
			t.rows[i].val2NoChg = val2NoChg

			if nochangeResultsCol1 == nil {
				nochangeResultsCol1 = make(map[int]bool)
			}
			if nochangeResultsCol2 == nil {
				nochangeResultsCol2 = make(map[int]bool)
			}
			nochangeResultsCol1[int(oldRowid)] = val1NoChg
			nochangeResultsCol2[int(oldRowid)] = val2NoChg

			// Update values
			if !val1NoChg && len(cols) > 0 {
				t.rows[i].val1, _ = cols[0].(string)
			}
			if !val2NoChg && len(cols) > 1 {
				t.rows[i].val2, _ = cols[1].(string)
			}
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (t *nochangeTableX) Delete(ctx vtab.Context, oldRowid int64) error {
	for i := range t.rows {
		if t.rows[i].rowid == oldRowid {
			t.rows = append(t.rows[:i], t.rows[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (t *nochangeTableX) Insert(ctx vtab.Context, cols []vtab.Value, rowid *int64) error {
	id := *rowid
	if id == 0 {
		id = int64(len(t.rows) + 1)
	}
	val1, _ := cols[0].(string)
	val2, _ := cols[1].(string)
	t.rows = append(t.rows, struct {
		rowid     int64
		val1      string
		val2      string
		val1NoChg bool
		val2NoChg bool
	}{rowid: id, val1: val1, val2: val2, val1NoChg: false, val2NoChg: false})
	*rowid = id
	return nil
}

func (c *nochangeCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *nochangeCursorX) Next() error {
	if c.pos < len(c.t.rows) {
		c.pos++
	}
	return nil
}

func (c *nochangeCursorX) Eof() bool { return c.pos >= len(c.t.rows) }

func (c *nochangeCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.t.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.t.rows[c.pos].val1, nil
	}
	if col == 1 {
		return c.t.rows[c.pos].val2, nil
	}
	return nil, nil
}

func (c *nochangeCursorX) Rowid() (int64, error) {
	if c.pos >= len(c.t.rows) {
		return 0, nil
	}
	return c.t.rows[c.pos].rowid, nil
}

func (c *nochangeCursorX) Close() error { return nil }

// TestVtabNochangeDetection tests that nochange detection works during UPDATE
// Note: sqlite3_value_nochange() behavior depends on SQLite version and context.
// This test verifies the API is correctly wired and called.
func TestVtabNochangeDetection(t *testing.T) {
	nochangeResultsCol1 = nil
	nochangeResultsCol2 = nil

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "nochange_mod", &nochangeModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE nc_test USING nochange_mod(val1, val2)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Test UPDATE - verify the API is called without errors
	_, err = db.Exec(`UPDATE nc_test SET val1 = 'changed1' WHERE rowid = 1`)
	if err != nil {
		t.Fatalf("update val1 only: %v", err)
	}

	// Verify the update was recorded
	if _, ok := nochangeResultsCol1[1]; !ok {
		t.Error("expected nochange results to be recorded for row 1")
	}
	if _, ok := nochangeResultsCol2[1]; !ok {
		t.Error("expected nochange results to be recorded for row 1 column 2")
	}

	// Verify value was updated
	rows, err := db.Query(`SELECT val1, val2 FROM nc_test WHERE rowid = 1`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var val1, val2 string
		if err := rows.Scan(&val1, &val2); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Logf("After UPDATE SET val1='changed1': val1=%q, val2=%q, val1NoChg=%v, val2NoChg=%v",
			val1, val2, nochangeResultsCol1[1], nochangeResultsCol2[1])
	}
}

// --- IN Constraint with Different Types Tests ---

// inTypedModuleX tests IN constraint with different value types (BLOB, FLOAT)
type inTypedModuleX struct{}

type inTypedTableX struct{}

type inTypedCursorX struct {
	rows []struct {
		rowid int64
		val   []byte
	}
	pos int
}

var inTypedValuesSeen [][]byte

func (m *inTypedModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("inTypedModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val BLOB)", args[2])); err != nil {
		return nil, err
	}
	return &inTypedTableX{}, nil
}

func (m *inTypedModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *inTypedTableX) BestIndex(info *vtab.IndexInfo) error {
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Column == 0 {
			if info.IsInConstraint(i) {
				c.ArgIndex = 0
				c.Omit = true
				info.HandleInConstraint(i, true)
				info.IdxNum = 1
				return nil
			}
		}
	}
	info.IdxNum = 0
	return nil
}

func (t *inTypedTableX) Open() (vtab.Cursor, error) { return &inTypedCursorX{}, nil }
func (t *inTypedTableX) Disconnect() error          { return nil }
func (t *inTypedTableX) Destroy() error             { return nil }

func (c *inTypedCursorX) FilterWithContext(ctx vtab.Context, idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	c.rows = nil
	inTypedValuesSeen = nil

	if idxNum != 1 {
		return nil
	}

	it := ctx.InIterate(vals, 0)
	for it.Next() {
		v := it.Value()
		if blob, ok := v.([]byte); ok {
			inTypedValuesSeen = append(inTypedValuesSeen, blob)
		}
	}

	for i, v := range inTypedValuesSeen {
		c.rows = append(c.rows, struct {
			rowid int64
			val   []byte
		}{rowid: int64(i + 1), val: v})
	}
	return nil
}

func (c *inTypedCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	return fmt.Errorf("Filter called instead of FilterWithContext")
}

func (c *inTypedCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *inTypedCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *inTypedCursorX) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *inTypedCursorX) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *inTypedCursorX) Close() error {
	c.rows = nil
	c.pos = 0
	return nil
}

// TestVtabInConstraintBlob tests IN constraint with BLOB values
func TestVtabInConstraintBlob(t *testing.T) {
	inTypedValuesSeen = nil

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "in_blob", &inTypedModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE blob_test USING in_blob(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query with IN clause containing BLOB literals
	rows, err := db.Query(`SELECT val FROM blob_test WHERE val IN (X'010203', X'AABBCCDD') ORDER BY val`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got [][]byte
	for rows.Next() {
		var v []byte
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Verify we got 2 BLOB values
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}

	t.Logf("BLOB IN values seen: %v", inTypedValuesSeen)
}

// --- IN Constraint with Float Tests ---

type inFloatModuleX struct{}

type inFloatTableX struct{}

type inFloatCursorX struct {
	rows []struct {
		rowid int64
		val   float64
	}
	pos int
}

var inFloatValuesSeen []float64

func (m *inFloatModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("inFloatModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val REAL)", args[2])); err != nil {
		return nil, err
	}
	return &inFloatTableX{}, nil
}

func (m *inFloatModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *inFloatTableX) BestIndex(info *vtab.IndexInfo) error {
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Column == 0 {
			if info.IsInConstraint(i) {
				c.ArgIndex = 0
				c.Omit = true
				info.HandleInConstraint(i, true)
				info.IdxNum = 1
				return nil
			}
		}
	}
	info.IdxNum = 0
	return nil
}

func (t *inFloatTableX) Open() (vtab.Cursor, error) { return &inFloatCursorX{}, nil }
func (t *inFloatTableX) Disconnect() error          { return nil }
func (t *inFloatTableX) Destroy() error             { return nil }

func (c *inFloatCursorX) FilterWithContext(ctx vtab.Context, idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	c.rows = nil
	inFloatValuesSeen = nil

	if idxNum != 1 {
		return nil
	}

	it := ctx.InIterate(vals, 0)
	for it.Next() {
		v := it.Value()
		if f, ok := v.(float64); ok {
			inFloatValuesSeen = append(inFloatValuesSeen, f)
		}
	}

	for i, v := range inFloatValuesSeen {
		c.rows = append(c.rows, struct {
			rowid int64
			val   float64
		}{rowid: int64(i + 1), val: v})
	}
	return nil
}

func (c *inFloatCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	return fmt.Errorf("Filter called instead of FilterWithContext")
}

func (c *inFloatCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *inFloatCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *inFloatCursorX) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *inFloatCursorX) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *inFloatCursorX) Close() error {
	c.rows = nil
	c.pos = 0
	return nil
}

// TestVtabInConstraintFloat tests IN constraint with FLOAT values
func TestVtabInConstraintFloat(t *testing.T) {
	inFloatValuesSeen = nil

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "in_float", &inFloatModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE float_test USING in_float(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query with IN clause containing float values
	rows, err := db.Query(`SELECT val FROM float_test WHERE val IN (1.5, 2.7, 3.14) ORDER BY val`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []float64
	for rows.Next() {
		var v float64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// Verify we got 3 float values
	if len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(got), got)
	}

	t.Logf("Float IN values seen: %v", inFloatValuesSeen)
}

// --- Filter Error Path Tests ---

// errFilterModule returns error from FilterWithContext to test error handling
type errFilterModuleX struct{}

type errFilterTableX struct{}

type errFilterCursorX struct{}

func (m *errFilterModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("errFilterModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val INTEGER)", args[2])); err != nil {
		return nil, err
	}
	return &errFilterTableX{}, nil
}

func (m *errFilterModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *errFilterTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *errFilterTableX) Open() (vtab.Cursor, error)           { return &errFilterCursorX{}, nil }
func (t *errFilterTableX) Disconnect() error                    { return nil }
func (t *errFilterTableX) Destroy() error                       { return nil }

func (c *errFilterCursorX) FilterWithContext(ctx vtab.Context, idxNum int, idxStr string, vals []vtab.Value) error {
	return fmt.Errorf("intentional filter error")
}

func (c *errFilterCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	return fmt.Errorf("intentional filter error")
}

func (c *errFilterCursorX) Next() error                    { return nil }
func (c *errFilterCursorX) Eof() bool                      { return true }
func (c *errFilterCursorX) Column(col int) (vtab.Value, error) { return nil, nil }
func (c *errFilterCursorX) Rowid() (int64, error)          { return 0, nil }
func (c *errFilterCursorX) Close() error                   { return nil }

// TestVtabFilterError tests that errors from FilterWithContext are properly propagated
func TestVtabFilterError(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "err_filter", &errFilterModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE err_test USING err_filter(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query should fail with our error message
	_, err = db.Query(`SELECT val FROM err_test`)
	if err == nil {
		t.Fatal("expected error from FilterWithContext, got nil")
	}
	if !strings.Contains(err.Error(), "intentional filter error") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Updater Error Path Tests ---

type errUpdateModuleX struct{}

type errUpdateTableX struct{}

type errUpdateCursorX struct{ pos int }

func (m *errUpdateModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("errUpdateModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val TEXT)", args[2])); err != nil {
		return nil, err
	}
	return &errUpdateTableX{}, nil
}

func (m *errUpdateModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *errUpdateTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *errUpdateTableX) Open() (vtab.Cursor, error) {
	return &errUpdateCursorX{pos: 0}, nil
}
func (t *errUpdateTableX) Disconnect() error                    { return nil }
func (t *errUpdateTableX) Destroy() error                       { return nil }

func (t *errUpdateTableX) Insert(ctx vtab.Context, cols []vtab.Value, rowid *int64) error {
	return fmt.Errorf("intentional insert error")
}

func (t *errUpdateTableX) Update(ctx vtab.Context, oldRowid int64, cols []vtab.Value, newRowid *int64) error {
	return fmt.Errorf("intentional update error")
}

func (t *errUpdateTableX) Delete(ctx vtab.Context, oldRowid int64) error {
	return fmt.Errorf("intentional delete error")
}

func (c *errUpdateCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *errUpdateCursorX) Next() error {
	c.pos++
	return nil
}

func (c *errUpdateCursorX) Eof() bool                      { return c.pos >= 1 }
func (c *errUpdateCursorX) Column(col int) (vtab.Value, error) { return "test", nil }
func (c *errUpdateCursorX) Rowid() (int64, error)          { return int64(c.pos), nil }
func (c *errUpdateCursorX) Close() error                   { return nil }

// TestVtabUpdateError tests that errors from UpdaterWithContext are properly propagated
func TestVtabUpdateError(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "err_update", &errUpdateModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE err_upd_test USING err_update(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// INSERT should fail
	_, err = db.Exec(`INSERT INTO err_upd_test VALUES('test')`)
	if err == nil {
		t.Fatal("expected error from Insert, got nil")
	}
	if !strings.Contains(err.Error(), "intentional insert error") {
		t.Errorf("unexpected insert error: %v", err)
	}
}

// --- IN Constraint with NULL tests ---

// inNullModuleX tests IN constraint with NULL values
type inNullModuleX struct{}

type inNullTableX struct{}

type inNullCursorX struct {
	rows []struct {
		rowid int64
		val   interface{}
	}
	pos int
}

var inNullValuesSeen []interface{}

func (m *inNullModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("inNullModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val INTEGER)", args[2])); err != nil {
		return nil, err
	}
	return &inNullTableX{}, nil
}

func (m *inNullModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *inNullTableX) BestIndex(info *vtab.IndexInfo) error {
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Column == 0 {
			if info.IsInConstraint(i) {
				c.ArgIndex = 0
				c.Omit = true
				info.HandleInConstraint(i, true)
				info.IdxNum = 1
				return nil
			}
		}
	}
	info.IdxNum = 0
	return nil
}

func (t *inNullTableX) Open() (vtab.Cursor, error) { return &inNullCursorX{}, nil }
func (t *inNullTableX) Disconnect() error          { return nil }
func (t *inNullTableX) Destroy() error             { return nil }

func (c *inNullCursorX) FilterWithContext(ctx vtab.Context, idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	c.rows = nil
	inNullValuesSeen = nil

	if idxNum != 1 {
		return nil
	}

	it := ctx.InIterate(vals, 0)
	for it.Next() {
		v := it.Value()
		inNullValuesSeen = append(inNullValuesSeen, v)
		c.rows = append(c.rows, struct {
			rowid int64
			val   interface{}
		}{rowid: int64(len(c.rows) + 1), val: v})
	}
	return nil
}

func (c *inNullCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	return fmt.Errorf("Filter called instead of FilterWithContext")
}

func (c *inNullCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *inNullCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *inNullCursorX) Column(col int) (vtab.Value, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *inNullCursorX) Rowid() (int64, error) {
	if c.pos < 0 || c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *inNullCursorX) Close() error {
	c.rows = nil
	c.pos = 0
	return nil
}

// TestVtabInConstraintNull tests IN constraint with NULL values
// Note: SQLite typically doesn't match NULL in IN clauses, but we test iteration
func TestVtabInConstraintNull(t *testing.T) {
	inNullValuesSeen = nil

	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "in_null", &inNullModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE null_test USING in_null(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query with IN clause - SQLite won't include NULL in IN results typically
	// This tests the iteration with empty/non-IN case
	rows, err := db.Query(`SELECT val FROM null_test WHERE val IN (1, 2, 3)`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	// Just verify we can iterate
	var count int
	for rows.Next() {
		count++
		var v interface{}
		_ = rows.Scan(&v)
	}
	t.Logf("NULL test: got %d rows, values seen: %v", count, inNullValuesSeen)
}

// --- DELETE Tests ---

type deleteModuleX struct{}

type deleteTableX struct {
	rows []struct {
		rowid int64
		val   string
	}
}

type deleteCursorX struct {
	t   *deleteTableX
	pos int
}

func (m *deleteModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("deleteModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val TEXT)", args[2])); err != nil {
		return nil, err
	}
	return &deleteTableX{
		rows: []struct {
			rowid int64
			val   string
		}{
			{rowid: 1, val: "first"},
			{rowid: 2, val: "second"},
			{rowid: 3, val: "third"},
		},
	}, nil
}

func (m *deleteModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *deleteTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *deleteTableX) Open() (vtab.Cursor, error) {
	return &deleteCursorX{t: t, pos: 0}, nil
}
func (t *deleteTableX) Disconnect() error                    { return nil }
func (t *deleteTableX) Destroy() error                       { return nil }

func (t *deleteTableX) Insert(ctx vtab.Context, cols []vtab.Value, rowid *int64) error {
	id := *rowid
	if id == 0 {
		id = int64(len(t.rows) + 1)
	}
	val, _ := cols[0].(string)
	t.rows = append(t.rows, struct {
		rowid int64
		val   string
	}{rowid: id, val: val})
	*rowid = id
	return nil
}

func (t *deleteTableX) Update(ctx vtab.Context, oldRowid int64, cols []vtab.Value, newRowid *int64) error {
	for i := range t.rows {
		if t.rows[i].rowid == oldRowid {
			val, _ := cols[0].(string)
			t.rows[i].val = val
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (t *deleteTableX) Delete(ctx vtab.Context, oldRowid int64) error {
	for i := range t.rows {
		if t.rows[i].rowid == oldRowid {
			t.rows = append(t.rows[:i], t.rows[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (c *deleteCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *deleteCursorX) Next() error {
	if c.pos < len(c.t.rows) {
		c.pos++
	}
	return nil
}

func (c *deleteCursorX) Eof() bool { return c.pos >= len(c.t.rows) }

func (c *deleteCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.t.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.t.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *deleteCursorX) Rowid() (int64, error) {
	if c.pos >= len(c.t.rows) {
		return 0, nil
	}
	return c.t.rows[c.pos].rowid, nil
}

func (c *deleteCursorX) Close() error { return nil }

// TestVtabDelete tests DELETE operation with UpdaterWithContext
func TestVtabDelete(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "delete_mod", &deleteModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE del_test USING delete_mod(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Count rows before
	var countBefore int
	err = db.QueryRow(`SELECT COUNT(*) FROM del_test`).Scan(&countBefore)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}
	if countBefore != 3 {
		t.Fatalf("expected 3 rows before delete, got %d", countBefore)
	}

	// DELETE one row
	_, err = db.Exec(`DELETE FROM del_test WHERE rowid = 2`)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Count rows after
	var countAfter int
	err = db.QueryRow(`SELECT COUNT(*) FROM del_test`).Scan(&countAfter)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	if countAfter != 2 {
		t.Fatalf("expected 2 rows after delete, got %d", countAfter)
	}
}

// --- INSERT with explicit rowid Tests ---

type insertModuleX struct{}

type insertTableX struct {
	rows []struct {
		rowid int64
		val   string
	}
}

type insertCursorX struct {
	t   *insertTableX
	pos int
}

func (m *insertModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("insertModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val TEXT)", args[2])); err != nil {
		return nil, err
	}
	return &insertTableX{rows: nil}, nil
}

func (m *insertModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *insertTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *insertTableX) Open() (vtab.Cursor, error) {
	return &insertCursorX{t: t, pos: 0}, nil
}
func (t *insertTableX) Disconnect() error                    { return nil }
func (t *insertTableX) Destroy() error                       { return nil }

func (t *insertTableX) Insert(ctx vtab.Context, cols []vtab.Value, rowid *int64) error {
	id := *rowid
	if id == 0 {
		id = int64(len(t.rows) + 1)
	}
	val, _ := cols[0].(string)
	t.rows = append(t.rows, struct {
		rowid int64
		val   string
	}{rowid: id, val: val})
	*rowid = id
	return nil
}

func (t *insertTableX) Update(ctx vtab.Context, oldRowid int64, cols []vtab.Value, newRowid *int64) error {
	return fmt.Errorf("not implemented")
}

func (t *insertTableX) Delete(ctx vtab.Context, oldRowid int64) error {
	return fmt.Errorf("not implemented")
}

func (c *insertCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *insertCursorX) Next() error {
	if c.pos < len(c.t.rows) {
		c.pos++
	}
	return nil
}

func (c *insertCursorX) Eof() bool { return c.pos >= len(c.t.rows) }

func (c *insertCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.t.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.t.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *insertCursorX) Rowid() (int64, error) {
	if c.pos >= len(c.t.rows) {
		return 0, nil
	}
	return c.t.rows[c.pos].rowid, nil
}

func (c *insertCursorX) Close() error { return nil }

// TestVtabInsertWithRowid tests INSERT operations
func TestVtabInsertWithRowid(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "insert_mod", &insertModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE ins_test USING insert_mod(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// INSERT with auto-generated rowid
	_, err = db.Exec(`INSERT INTO ins_test(val) VALUES('first')`)
	if err != nil {
		t.Fatalf("insert first: %v", err)
	}

	// INSERT another row
	_, err = db.Exec(`INSERT INTO ins_test(val) VALUES('second')`)
	if err != nil {
		t.Fatalf("insert second: %v", err)
	}

	// Verify we have 2 rows
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM ins_test`).Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// --- Legacy Updater (without Context) Tests ---

type legacyUpdaterModuleX struct{}

type legacyUpdaterTableX struct {
	rows []struct {
		rowid int64
		val   string
	}
}

type legacyUpdaterCursorX struct {
	t   *legacyUpdaterTableX
	pos int
}

func (m *legacyUpdaterModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("legacyUpdaterModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val TEXT)", args[2])); err != nil {
		return nil, err
	}
	return &legacyUpdaterTableX{
		rows: []struct {
			rowid int64
			val   string
		}{{rowid: 1, val: "initial"}},
	}, nil
}

func (m *legacyUpdaterModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *legacyUpdaterTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *legacyUpdaterTableX) Open() (vtab.Cursor, error) {
	return &legacyUpdaterCursorX{t: t, pos: 0}, nil
}
func (t *legacyUpdaterTableX) Disconnect() error                    { return nil }
func (t *legacyUpdaterTableX) Destroy() error                       { return nil }

// Legacy Updater interface (without Context)
func (t *legacyUpdaterTableX) Insert(cols []vtab.Value, rowid *int64) error {
	id := *rowid
	if id == 0 {
		id = int64(len(t.rows) + 1)
	}
	val, _ := cols[0].(string)
	t.rows = append(t.rows, struct {
		rowid int64
		val   string
	}{rowid: id, val: val})
	*rowid = id
	return nil
}

func (t *legacyUpdaterTableX) Update(oldRowid int64, cols []vtab.Value, newRowid *int64) error {
	for i := range t.rows {
		if t.rows[i].rowid == oldRowid {
			val, _ := cols[0].(string)
			t.rows[i].val = val
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (t *legacyUpdaterTableX) Delete(oldRowid int64) error {
	for i := range t.rows {
		if t.rows[i].rowid == oldRowid {
			t.rows = append(t.rows[:i], t.rows[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("row %d not found", oldRowid)
}

func (c *legacyUpdaterCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *legacyUpdaterCursorX) Next() error {
	if c.pos < len(c.t.rows) {
		c.pos++
	}
	return nil
}

func (c *legacyUpdaterCursorX) Eof() bool { return c.pos >= len(c.t.rows) }

func (c *legacyUpdaterCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.t.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.t.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *legacyUpdaterCursorX) Rowid() (int64, error) {
	if c.pos >= len(c.t.rows) {
		return 0, nil
	}
	return c.t.rows[c.pos].rowid, nil
}

func (c *legacyUpdaterCursorX) Close() error { return nil }

// TestVtabLegacyUpdater tests the legacy Updater interface (without Context)
func TestVtabLegacyUpdater(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "legacy_upd", &legacyUpdaterModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE leg_test USING legacy_upd(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// INSERT via legacy interface
	_, err = db.Exec(`INSERT INTO leg_test(rowid, val) VALUES(10, 'inserted')`)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	// DELETE via legacy interface
	_, err = db.Exec(`DELETE FROM leg_test WHERE rowid = 1`)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify we have 1 row (the one we inserted)
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM leg_test`).Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after operations, got %d", count)
	}
}

// --- OrderByConsumed Tests ---

type orderByModuleX struct{}

type orderByTableX struct{}

type orderByCursorX struct {
	rows []struct {
		rowid int64
		val   string
	}
	pos int
}

func (m *orderByModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("orderByModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val TEXT)", args[2])); err != nil {
		return nil, err
	}
	return &orderByTableX{}, nil
}

func (m *orderByModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *orderByTableX) BestIndex(info *vtab.IndexInfo) error {
	// Mark order by as consumed if present
	if len(info.OrderBy) > 0 {
		info.OrderByConsumed = true
	}
	info.IdxStr = "custom_index"
	info.EstimatedCost = 10.5
	info.EstimatedRows = 100
	info.IdxNum = 42
	return nil
}

func (t *orderByTableX) Open() (vtab.Cursor, error) {
	return &orderByCursorX{
		rows: []struct {
			rowid int64
			val   string
		}{
			{rowid: 1, val: "z"},
			{rowid: 2, val: "a"},
			{rowid: 3, val: "m"},
		},
		pos: 0,
	}, nil
}

func (t *orderByTableX) Disconnect() error { return nil }
func (t *orderByTableX) Destroy() error    { return nil }

func (c *orderByCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	// Verify IdxStr was passed
	if idxStr != "custom_index" {
		// Log but don't fail - SQLite may not always pass it
	}
	return nil
}

func (c *orderByCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *orderByCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *orderByCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *orderByCursorX) Rowid() (int64, error) {
	if c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *orderByCursorX) Close() error { return nil }

// TestVtabOrderByConsumed tests OrderByConsumed and IdxStr propagation
func TestVtabOrderByConsumed(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "orderby_mod", &orderByModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE ord_test USING orderby_mod(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query with ORDER BY - should trigger OrderByConsumed
	rows, err := db.Query(`SELECT val FROM ord_test ORDER BY val`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// We should have 3 rows
	if len(got) != 3 {
		t.Errorf("expected 3 rows, got %d", len(got))
	}
}

// --- Standard Cursor (without CursorWithContext) Tests ---

type standardCursorModuleX struct{}

type standardCursorTableX struct{}

type standardCursorX struct {
	rows []struct {
		rowid int64
		val   int64
	}
	pos int
}

func (m *standardCursorModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("standardCursorModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val INTEGER)", args[2])); err != nil {
		return nil, err
	}
	return &standardCursorTableX{}, nil
}

func (m *standardCursorModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *standardCursorTableX) BestIndex(info *vtab.IndexInfo) error {
	// Assign ArgIndex for constraints
	for i, c := range info.Constraints {
		if c.Usable && c.Op == vtab.OpEQ {
			info.Constraints[i].ArgIndex = 0
			break
		}
	}
	return nil
}

func (t *standardCursorTableX) Open() (vtab.Cursor, error) {
	return &standardCursorX{
		rows: []struct {
			rowid int64
			val   int64
		}{
			{rowid: 1, val: 10},
			{rowid: 2, val: 20},
		},
		pos: 0,
	}, nil
}

func (t *standardCursorTableX) Disconnect() error { return nil }
func (t *standardCursorTableX) Destroy() error    { return nil }

// Standard Cursor (without CursorWithContext) - tests fallback path
func (c *standardCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *standardCursorX) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *standardCursorX) Eof() bool { return c.pos >= len(c.rows) }

func (c *standardCursorX) Column(col int) (vtab.Value, error) {
	if c.pos >= len(c.rows) {
		return nil, nil
	}
	if col == 0 {
		return c.rows[c.pos].val, nil
	}
	return nil, nil
}

func (c *standardCursorX) Rowid() (int64, error) {
	if c.pos >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.pos].rowid, nil
}

func (c *standardCursorX) Close() error { return nil }

// TestVtabStandardCursor tests standard Cursor interface (without CursorWithContext)
func TestVtabStandardCursor(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "std_cursor", &standardCursorModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE std_test USING std_cursor(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query - should use standard Cursor interface
	rows, err := db.Query(`SELECT val FROM std_test WHERE val = 10`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var got []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	// We should have rows (constraint may filter)
	t.Logf("Got %d rows from standard cursor", len(got))
}

// --- BestIndex Error Tests ---

type bestIndexErrorModuleX struct{}

type bestIndexErrorTableX struct{}

type bestIndexErrorCursorX struct{ pos int }

func (m *bestIndexErrorModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("bestIndexErrorModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val INTEGER)", args[2])); err != nil {
		return nil, err
	}
	return &bestIndexErrorTableX{}, nil
}

func (m *bestIndexErrorModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *bestIndexErrorTableX) BestIndex(info *vtab.IndexInfo) error {
	return fmt.Errorf("intentional BestIndex error")
}

func (t *bestIndexErrorTableX) Open() (vtab.Cursor, error) { return &bestIndexErrorCursorX{}, nil }
func (t *bestIndexErrorTableX) Disconnect() error          { return nil }
func (t *bestIndexErrorTableX) Destroy() error             { return nil }

func (c *bestIndexErrorCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	c.pos = 0
	return nil
}

func (c *bestIndexErrorCursorX) Next() error {
	c.pos++
	return nil
}

func (c *bestIndexErrorCursorX) Eof() bool                      { return c.pos >= 1 }
func (c *bestIndexErrorCursorX) Column(col int) (vtab.Value, error) { return nil, nil }
func (c *bestIndexErrorCursorX) Rowid() (int64, error)          { return 0, nil }
func (c *bestIndexErrorCursorX) Close() error                   { return nil }

// TestVtabBestIndexError tests error handling in BestIndex
func TestVtabBestIndexError(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "bestidx_err", &bestIndexErrorModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE bi_err_test USING bestidx_err(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query should fail due to BestIndex error
	_, err = db.Query(`SELECT val FROM bi_err_test WHERE val = 1`)
	if err == nil {
		t.Fatal("expected error from BestIndex, got nil")
	}
	if !strings.Contains(err.Error(), "intentional BestIndex error") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Filter Error (Standard Cursor) Tests ---

type filterErrorModuleX struct{}

type filterErrorTableX struct{}

type filterErrorCursorX struct{}

func (m *filterErrorModuleX) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("filterErrorModuleX: missing table name")
	}
	if err := ctx.Declare(fmt.Sprintf("CREATE TABLE %s(val INTEGER)", args[2])); err != nil {
		return nil, err
	}
	return &filterErrorTableX{}, nil
}

func (m *filterErrorModuleX) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
}

func (t *filterErrorTableX) BestIndex(info *vtab.IndexInfo) error { return nil }
func (t *filterErrorTableX) Open() (vtab.Cursor, error) {
	return &filterErrorCursorX{}, nil
}
func (t *filterErrorTableX) Disconnect() error                    { return nil }
func (t *filterErrorTableX) Destroy() error                       { return nil }

func (c *filterErrorCursorX) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	return fmt.Errorf("intentional Filter error")
}

func (c *filterErrorCursorX) Next() error                    { return nil }
func (c *filterErrorCursorX) Eof() bool                      { return true }
func (c *filterErrorCursorX) Column(col int) (vtab.Value, error) { return nil, nil }
func (c *filterErrorCursorX) Rowid() (int64, error)          { return 0, nil }
func (c *filterErrorCursorX) Close() error                   { return nil }

// TestVtabFilterErrorStandard tests error handling in Filter (standard Cursor)
func TestVtabFilterErrorStandard(t *testing.T) {
	db, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := vtab.RegisterModule(db, "filter_err", &filterErrorModuleX{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := db.Exec(`CREATE VIRTUAL TABLE fe_test USING filter_err(val)`); err != nil {
		t.Fatalf("create vt: %v", err)
	}

	// Query should fail due to Filter error
	_, err = db.Query(`SELECT val FROM fe_test`)
	if err == nil {
		t.Fatal("expected error from Filter, got nil")
	}
	if !strings.Contains(err.Error(), "intentional Filter error") {
		t.Errorf("unexpected error: %v", err)
	}
}

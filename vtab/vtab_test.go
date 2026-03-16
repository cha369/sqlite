package vtab

import (
	"database/sql/driver"
	"errors"
	"testing"
)

// TestContextExecNotAvailable 测试当 execSQL 为 nil 时，Exec 返回错误
func TestContextExecNotAvailable(t *testing.T) {
	ctx := NewContext(func(string) error { return nil })

	err := ctx.Exec("CREATE TABLE test (id INTEGER)")
	if err == nil {
		t.Error("Expected error when Exec is not available, got nil")
	}
	if err.Error() != "vtab: Exec not available in this context" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// TestContextExecAvailable 测试当 execSQL 设置时，Exec 正常工作
func TestContextExecAvailable(t *testing.T) {
	executed := false
	var receivedSQL string
	var receivedArgs []driver.Value

	execFn := func(sql string, args []driver.Value) error {
		executed = true
		receivedSQL = sql
		receivedArgs = args
		return nil
	}

	ctx := NewContextWithExec(
		func(string) error { return nil },
		nil,
		nil,
		execFn,
	)

	err := ctx.Exec("INSERT INTO test VALUES (?)", 42)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !executed {
		t.Error("Exec function was not called")
	}

	if receivedSQL != "INSERT INTO test VALUES (?)" {
		t.Errorf("Unexpected SQL: %s", receivedSQL)
	}

	if len(receivedArgs) != 1 || receivedArgs[0] != 42 {
		t.Errorf("Unexpected args: %v", receivedArgs)
	}
}

// TestContextExecErrorPropagation 测试 Exec 错误传递
func TestContextExecErrorPropagation(t *testing.T) {
	expectedErr := errors.New("database locked")

	execFn := func(sql string, args []driver.Value) error {
		return expectedErr
	}

	ctx := NewContextWithExec(
		func(string) error { return nil },
		nil,
		nil,
		execFn,
	)

	err := ctx.Exec("SELECT 1")
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

// TestNewContextWithExecAllFields 测试完整配置的 Context
func TestNewContextWithExecAllFields(t *testing.T) {
	declareCalled := false
	constraintCalled := false
	configCalled := false
	execCalled := false

	ctx := NewContextWithExec(
		func(string) error { declareCalled = true; return nil },
		func() error { constraintCalled = true; return nil },
		func(int32, ...int32) error { configCalled = true; return nil },
		func(string, []driver.Value) error { execCalled = true; return nil },
	)

	// 测试所有方法
	ctx.Declare("CREATE TABLE t (x)")
	ctx.EnableConstraintSupport()
	ctx.Config(1)
	ctx.Exec("SELECT 1")

	if !declareCalled {
		t.Error("Declare was not called")
	}
	if !constraintCalled {
		t.Error("EnableConstraintSupport was not called")
	}
	if !configCalled {
		t.Error("Config was not called")
	}
	if !execCalled {
		t.Error("Exec was not called")
	}
}

// TestContextExecWithMultipleArgs 测试 Exec 带多个参数
func TestContextExecWithMultipleArgs(t *testing.T) {
	var capturedArgs []driver.Value

	execFn := func(sql string, args []driver.Value) error {
		capturedArgs = args
		return nil
	}

	ctx := NewContextWithExec(
		func(string) error { return nil },
		nil,
		nil,
		execFn,
	)

	err := ctx.Exec("INSERT INTO t VALUES (?, ?, ?)", 1, "hello", 3.14)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(capturedArgs) != 3 {
		t.Errorf("Expected 3 args, got %d", len(capturedArgs))
	}

	if capturedArgs[0] != 1 || capturedArgs[1] != "hello" || capturedArgs[2] != 3.14 {
		t.Errorf("Unexpected args: %v", capturedArgs)
	}
}

// TestContextExecNoArgs 测试 Exec 不带参数
func TestContextExecNoArgs(t *testing.T) {
	var capturedArgs []driver.Value

	execFn := func(sql string, args []driver.Value) error {
		capturedArgs = args
		return nil
	}

	ctx := NewContextWithExec(
		func(string) error { return nil },
		nil,
		nil,
		execFn,
	)

	err := ctx.Exec("CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(capturedArgs) != 0 {
		t.Errorf("Expected 0 args, got %d", len(capturedArgs))
	}
}

// TestBlobReadWriteClose 测试 Blob 的读写关闭操作
func TestBlobReadWriteClose(t *testing.T) {
	var writtenData []byte
	var readOffset int64
	var closeCalled bool

	blob := NewBlob(
		1, // handle
		func(handle uintptr, offset int64, p []byte) error {
			readOffset = offset
			copy(p, []byte("test data"))
			return nil
		},
		func(handle uintptr, offset int64, p []byte) error {
			writtenData = make([]byte, len(p))
			copy(writtenData, p)
			return nil
		},
		func(handle uintptr) error {
			closeCalled = true
			return nil
		},
	)

	// Test Write
	n, err := blob.Write(10, []byte("hello"))
	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != 5 {
		t.Errorf("Write returned %d, expected 5", n)
	}
	if string(writtenData) != "hello" {
		t.Errorf("Write data: got %q, want %q", writtenData, "hello")
	}

	// Test Read
	buf := make([]byte, 9)
	n, err = blob.Read(5, buf)
	if err != nil {
		t.Errorf("Read failed: %v", err)
	}
	if n != 9 {
		t.Errorf("Read returned %d, expected 9", n)
	}
	if readOffset != 5 {
		t.Errorf("Read offset: got %d, want 5", readOffset)
	}

	// Test Close
	if err := blob.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	if !closeCalled {
		t.Error("Close was not called")
	}

	// Test operations on closed blob
	_, err = blob.Read(0, buf)
	if err == nil {
		t.Error("Expected error on closed blob Read")
	}
	_, err = blob.Write(0, []byte("x"))
	if err == nil {
		t.Error("Expected error on closed blob Write")
	}
}

// TestBlobDoubleClose 测试 Blob 重复关闭
func TestBlobDoubleClose(t *testing.T) {
	closeCount := 0
	blob := NewBlob(
		1,
		func(uintptr, int64, []byte) error { return nil },
		func(uintptr, int64, []byte) error { return nil },
		func(uintptr) error { closeCount++; return nil },
	)

	// First close
	if err := blob.Close(); err != nil {
		t.Errorf("First close failed: %v", err)
	}
	// Second close should be no-op
	if err := blob.Close(); err != nil {
		t.Errorf("Second close failed: %v", err)
	}
	if closeCount != 1 {
		t.Errorf("Close called %d times, expected 1", closeCount)
	}
}

// TestContextOpenBlob 测试 OpenBlob 方法
func TestContextOpenBlob(t *testing.T) {
	// Test without openBlob
	ctx := NewContext(func(string) error { return nil })
	_, err := ctx.OpenBlob("main", "t", "data", 1, true)
	if err == nil {
		t.Error("Expected error when OpenBlob not available")
	}

	// Test with openBlob
	openCalled := false
	ctx2 := Context{
		openBlob: func(db, table, column string, rowid int64, write bool) (*Blob, error) {
			openCalled = true
			if db != "main" || table != "chunks" || column != "vector" || rowid != 42 || !write {
				t.Errorf("OpenBlob args: db=%q, table=%q, column=%q, rowid=%d, write=%v",
					db, table, column, rowid, write)
			}
			return &Blob{handle: 1}, nil
		},
	}
	blob, err := ctx2.OpenBlob("main", "chunks", "vector", 42, true)
	if err != nil {
		t.Errorf("OpenBlob failed: %v", err)
	}
	if !openCalled {
		t.Error("openBlob was not called")
	}
	if blob == nil {
		t.Error("Expected non-nil blob")
	}
}

// TestContextValueNoChange 测试 ValueNoChange 方法
func TestContextValueNoChange(t *testing.T) {
	// Test without noChangeCheck
	ctx := NewContext(func(string) error { return nil })
	if ctx.ValueNoChange(0) {
		t.Error("Expected false when noChangeCheck not available")
	}

	// Test with noChangeCheck
	checks := make(map[int]bool)
	ctx2 := Context{
		noChangeCheck: func(colIndex int) bool {
			return checks[colIndex]
		},
	}

	checks[0] = true
	checks[1] = false

	if !ctx2.ValueNoChange(0) {
		t.Error("Expected true for column 0")
	}
	if ctx2.ValueNoChange(1) {
		t.Error("Expected false for column 1")
	}
	if ctx2.ValueNoChange(-1) {
		t.Error("Expected false for negative index")
	}
}

// TestInIterator 测试 IN 迭代器
func TestInIterator(t *testing.T) {
	values := []driver.Value{int64(1), int64(2), int64(3)}
	idx := 0

	ctx := Context{
		inFirst: func(valPtr uintptr) (driver.Value, bool) {
			idx = 0
			if idx < len(values) {
				v := values[idx]
				idx++
				return v, true
			}
			return nil, false
		},
		inNext: func(valPtr uintptr) (driver.Value, bool) {
			if idx < len(values) {
				v := values[idx]
				idx++
				return v, true
			}
			return nil, false
		},
		valPtrs: []uintptr{1},
	}

	it := ctx.InIterate([]driver.Value{}, 0)

	var got []int64
	for it.Next() {
		v := it.Value()
		if v == nil {
			t.Error("Got nil value during iteration")
			break
		}
		got = append(got, v.(int64))
	}

	if len(got) != 3 {
		t.Errorf("Expected 3 values, got %d: %v", len(got), got)
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("Unexpected values: %v", got)
	}

	// After iteration ends, Next returns false
	if it.Next() {
		t.Error("Expected Next to return false after iteration ended")
	}
}

// TestInIteratorNotAvailable 测试 IN 迭代器不可用时
func TestInIteratorNotAvailable(t *testing.T) {
	ctx := NewContext(func(string) error { return nil })
	it := ctx.InIterate([]driver.Value{}, 0)

	if it.Next() {
		t.Error("Expected false when InIterate not available")
	}
}

// TestInIteratorWithInvalidIndex 测试无效索引
func TestInIteratorWithInvalidIndex(t *testing.T) {
	ctx := Context{
		inFirst: func(uintptr) (driver.Value, bool) { return int64(1), true },
		inNext:  func(uintptr) (driver.Value, bool) { return nil, false },
		valPtrs: []uintptr{1},
	}

	// Negative index
	it := ctx.InIterate([]driver.Value{}, -1)
	if it.Next() {
		t.Error("Expected false for negative index")
	}

	// Out of range index
	it = ctx.InIterate([]driver.Value{}, 10)
	if it.Next() {
		t.Error("Expected false for out of range index")
	}
}

// TestIndexInfoIsInConstraint 测试 IsInConstraint 方法
func TestIndexInfoIsInConstraint(t *testing.T) {
	constraints := []Constraint{
		{Column: 0, IsIn: true},
		{Column: 1, IsIn: false},
	}

	info := &IndexInfo{
		Constraints: constraints,
		_isIn: func(iCons int) bool {
			if iCons < 0 || iCons >= len(constraints) {
				return false
			}
			return constraints[iCons].IsIn
		},
	}

	if !info.IsInConstraint(0) {
		t.Error("Expected constraint 0 to be IN")
	}
	if info.IsInConstraint(1) {
		t.Error("Expected constraint 1 not to be IN")
	}
	if info.IsInConstraint(-1) {
		t.Error("Expected false for negative index")
	}
	if info.IsInConstraint(10) {
		t.Error("Expected false for out of range index")
	}
}

// TestIndexInfoHandleInConstraint 测试 HandleInConstraint 方法
func TestIndexInfoHandleInConstraint(t *testing.T) {
	var handled []int

	info := &IndexInfo{
		Constraints: []Constraint{
			{Column: 0},
			{Column: 1},
		},
		_handleIn: func(iCons int, handle int) {
			handled = append(handled, handle)
		},
	}

	info.HandleInConstraint(0, true)
	info.HandleInConstraint(1, false)

	if len(handled) != 2 {
		t.Fatalf("Expected 2 calls, got %d", len(handled))
	}
	if handled[0] != 1 {
		t.Errorf("First call: got %d, want 1", handled[0])
	}
	if handled[1] != 0 {
		t.Errorf("Second call: got %d, want 0", handled[1])
	}
}

// TestIndexInfoHandleInConstraintWithoutFunc 测试无回调时的 HandleInConstraint
func TestIndexInfoHandleInConstraintWithoutFunc(t *testing.T) {
	info := &IndexInfo{
		Constraints: []Constraint{{Column: 0}},
	}

	// Should not panic
	info.HandleInConstraint(0, true)
	info.HandleInConstraint(-1, true)
}

// TestNewContextForUpdate 测试 NewContextForUpdate
func TestNewContextForUpdate(t *testing.T) {
	base := NewContext(func(string) error { return nil })
	cfg := &UpdateContextConfig{
		NoChangeCheck: func(colIndex int) bool { return colIndex == 0 },
	}

	ctx := NewContextForUpdate(base, cfg)

	if !ctx.ValueNoChange(0) {
		t.Error("Expected ValueNoChange(0) to return true")
	}
	if ctx.ValueNoChange(1) {
		t.Error("Expected ValueNoChange(1) to return false")
	}
}

// TestNewContextForFilter 测试 NewContextForFilter
func TestNewContextForFilter(t *testing.T) {
	base := NewContext(func(string) error { return nil })
	cfg := &FilterContextConfig{
		ValPtrs: []uintptr{1, 2},
		InFirst: func(uintptr) (driver.Value, bool) { return int64(1), true },
		InNext:  func(uintptr) (driver.Value, bool) { return nil, false },
	}

	ctx := NewContextForFilter(base, cfg)

	if ctx.valPtrs == nil || len(ctx.valPtrs) != 2 {
		t.Errorf("valPtrs not set correctly: %v", ctx.valPtrs)
	}
	if ctx.inFirst == nil {
		t.Error("inFirst not set")
	}
	if ctx.inNext == nil {
		t.Error("inNext not set")
	}
}

// TestConstraintIsInField 测试 Constraint.IsIn 字段
func TestConstraintIsInField(t *testing.T) {
	c := Constraint{
		Column:   0,
		Op:       OpEQ,
		Usable:   true,
		ArgIndex: -1,
		Omit:     false,
		IsIn:     true,
	}

	if !c.IsIn {
		t.Error("Expected IsIn to be true")
	}
}

// TestNewContextWithConstraintSupport 测试带约束支持的 Context 创建
func TestNewContextWithConstraintSupport(t *testing.T) {
	declareCalled := false
	constraintCalled := false

	ctx := NewContextWithConstraintSupport(
		func(s string) error { declareCalled = true; return nil },
		func() error { constraintCalled = true; return nil },
	)

	if err := ctx.Declare("CREATE TABLE t (x)"); err != nil {
		t.Errorf("Declare failed: %v", err)
	}
	if !declareCalled {
		t.Error("Declare not called")
	}

	if err := ctx.EnableConstraintSupport(); err != nil {
		t.Errorf("EnableConstraintSupport failed: %v", err)
	}
	if !constraintCalled {
		t.Error("EnableConstraintSupport not called")
	}
}

// TestNewContextWithConfig 测试带配置的 Context 创建
func TestNewContextWithConfig(t *testing.T) {
	declareCalled := false
	constraintCalled := false
	configCalled := false

	ctx := NewContextWithConfig(
		func(s string) error { declareCalled = true; return nil },
		func() error { constraintCalled = true; return nil },
		func(op int32, args ...int32) error { configCalled = true; return nil },
	)

	if err := ctx.Declare("CREATE TABLE t (x)"); err != nil {
		t.Errorf("Declare failed: %v", err)
	}
	if err := ctx.EnableConstraintSupport(); err != nil {
		t.Errorf("EnableConstraintSupport failed: %v", err)
	}
	if err := ctx.Config(1); err != nil {
		t.Errorf("Config failed: %v", err)
	}

	if !declareCalled || !constraintCalled || !configCalled {
		t.Error("Not all callbacks were called")
	}
}

// TestNewContextWithBlob 测试带 Blob 操作的 Context 创建
func TestNewContextWithBlob(t *testing.T) {
	ops := &BlobOps{
		Open: func(db, table, column string, rowid int64, write bool) (*Blob, error) {
			return &Blob{handle: 1}, nil
		},
		Read:  func(uintptr, int64, []byte) error { return nil },
		Write: func(uintptr, int64, []byte) error { return nil },
		Close: func(uintptr) error { return nil },
	}

	ctx := NewContextWithBlob(
		func(string) error { return nil },
		func() error { return nil },
		func(int32, ...int32) error { return nil },
		func(string, []driver.Value) error { return nil },
		ops,
	)

	blob, err := ctx.OpenBlob("main", "t", "data", 1, true)
	if err != nil {
		t.Errorf("OpenBlob failed: %v", err)
	}
	if blob == nil {
		t.Error("Expected non-nil blob")
	}
}

// TestContextDeclareNotAvailable 测试 Declare 不可用时
func TestContextDeclareNotAvailable(t *testing.T) {
	ctx := Context{}
	err := ctx.Declare("CREATE TABLE t (x)")
	if err == nil {
		t.Error("Expected error when declare not available")
	}
	if err.Error() != "vtab: declare not available in this context" {
		t.Errorf("Unexpected error: %v", err)
	}
}

// TestContextEnableConstraintSupportNotAvailable 测试 EnableConstraintSupport 不可用时
func TestContextEnableConstraintSupportNotAvailable(t *testing.T) {
	ctx := Context{}
	err := ctx.EnableConstraintSupport()
	if err == nil {
		t.Error("Expected error when constraint support not available")
	}
}

// TestContextConfigNotAvailable 测试 Config 不可用时
func TestContextConfigNotAvailable(t *testing.T) {
	ctx := Context{}
	err := ctx.Config(1)
	if err == nil {
		t.Error("Expected error when config not available")
	}
}

// TestBlobReadError 测试 Blob 读错误
func TestBlobReadError(t *testing.T) {
	expectedErr := errors.New("read error")
	blob := NewBlob(
		1,
		func(uintptr, int64, []byte) error { return expectedErr },
		func(uintptr, int64, []byte) error { return nil },
		func(uintptr) error { return nil },
	)

	_, err := blob.Read(0, make([]byte, 10))
	if err != expectedErr {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}
}

// TestBlobWriteError 测试 Blob 写错误
func TestBlobWriteError(t *testing.T) {
	expectedErr := errors.New("write error")
	blob := NewBlob(
		1,
		func(uintptr, int64, []byte) error { return nil },
		func(uintptr, int64, []byte) error { return expectedErr },
		func(uintptr) error { return nil },
	)

	_, err := blob.Write(0, []byte("test"))
	if err != expectedErr {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}
}

// TestBlobCloseError 测试 Blob 关闭错误
func TestBlobCloseError(t *testing.T) {
	expectedErr := errors.New("close error")
	blob := NewBlob(
		1,
		func(uintptr, int64, []byte) error { return nil },
		func(uintptr, int64, []byte) error { return nil },
		func(uintptr) error { return expectedErr },
	)

	err := blob.Close()
	if err != expectedErr {
		t.Errorf("Expected %v, got %v", expectedErr, err)
	}
}

// TestInIteratorNilFunctions 测试 InIterator nil 函数情况
func TestInIteratorNilFunctions(t *testing.T) {
	// Test with nil inFirst
	it := &InIterator{
		inFirst: nil,
		done:    false,
	}
	if it.Next() {
		t.Error("Expected false when inFirst is nil")
	}

	// Test with nil inNext (after first call simulated)
	it2 := &InIterator{
		inFirst: func(uintptr) (driver.Value, bool) { return int64(1), true },
		inNext:  nil,
		current: int64(1), // Simulate after first call
		done:    false,
	}
	if it2.Next() {
		t.Error("Expected false when inNext is nil")
	}
}

// TestInIteratorEmpty 测试空迭代器
func TestInIteratorEmpty(t *testing.T) {
	it := &InIterator{
		inFirst: func(uintptr) (driver.Value, bool) { return nil, false },
		done:    false,
	}
	if it.Next() {
		t.Error("Expected false for empty iterator")
	}
}

// TestSetIsInFunc 测试 SetIsInFunc
func TestSetIsInFunc(t *testing.T) {
	info := &IndexInfo{}
	info.SetIsInFunc(func(iCons int) bool { return iCons == 0 })

	if info._isIn == nil {
		t.Error("_isIn function not set")
	}
	if !info._isIn(0) {
		t.Error("_isIn(0) should return true")
	}
}

// TestSetHandleInFunc 测试 SetHandleInFunc
func TestSetHandleInFunc(t *testing.T) {
	info := &IndexInfo{}
	info.SetHandleInFunc(func(iCons int, handle int) {})

	if info._handleIn == nil {
		t.Error("_handleIn function not set")
	}
}

// TestSetRegisterFunc 测试 SetRegisterFunc
func TestSetRegisterFunc(t *testing.T) {
	// Save original
	origHook := registerHook
	defer func() { registerHook = origHook }()

	called := false
	SetRegisterFunc(func(name string, m Module) error {
		called = true
		return nil
	})

	if registerHook == nil {
		t.Error("registerHook not set")
	}

	// Test that it's called
	registerHook("test", nil)
	if !called {
		t.Error("registerHook not called")
	}
}

// TestRegisterModule 测试 RegisterModule
func TestRegisterModule(t *testing.T) {
	// Save original
	origHook := registerHook
	defer func() { registerHook = origHook }()

	// Test with nil hook
	registerHook = nil
	err := RegisterModule(nil, "test", nil)
	if err != ErrNotImplemented {
		t.Errorf("Expected ErrNotImplemented, got %v", err)
	}

	// Test with empty name
	registerHook = func(name string, m Module) error { return nil }
	err = RegisterModule(nil, "", nil)
	if err == nil {
		t.Error("Expected error for empty name")
	}

	// Test with nil module
	err = RegisterModule(nil, "test", nil)
	if err == nil {
		t.Error("Expected error for nil module")
	}

	// Test successful registration
	var registeredName string
	var registeredModule Module
	registerHook = func(name string, m Module) error {
		registeredName = name
		registeredModule = m
		return nil
	}

	mockModule := &mockModule{}
	err = RegisterModule(nil, "mymodule", mockModule)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if registeredName != "mymodule" {
		t.Errorf("Expected name 'mymodule', got %q", registeredName)
	}
	if registeredModule != mockModule {
		t.Error("Module not registered correctly")
	}
}

// mockModule 用于测试
type mockModule struct{}

func (m *mockModule) Create(ctx Context, args []string) (Table, error)  { return nil, nil }
func (m *mockModule) Connect(ctx Context, args []string) (Table, error) { return nil, nil }

// TestValueNoChangeNegativeIndex 测试 ValueNoChange 负索引
func TestValueNoChangeNegativeIndex(t *testing.T) {
	ctx := Context{
		noChangeCheck: func(colIndex int) bool { return true },
	}
	// Negative index returns false
	if ctx.ValueNoChange(-1) {
		t.Error("Expected false for negative index")
	}
}

// TestInIterateWithNilValPtrs 测试 InIterate nil valPtrs
func TestInIterateWithNilValPtrs(t *testing.T) {
	ctx := Context{
		inFirst: func(uintptr) (driver.Value, bool) { return int64(1), true },
		valPtrs: nil,
	}

	it := ctx.InIterate([]driver.Value{}, 0)
	if it.Next() {
		t.Error("Expected false when valPtrs is nil")
	}
}

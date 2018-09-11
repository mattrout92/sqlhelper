package sqlhelper

import (
	"errors"
	"reflect"
)

// SQLRows represent the methods that can be actioned upon
// sql rows
type SQLRows interface {
	Next() bool
	Scan(...interface{}) error
	Columns() ([]string, error)
	Close() error
}

// SQLRow represents the methods that can be actioned upon
// an sql row
type SQLRow interface {
	Scan(...interface{}) error
	Columns() ([]string, error)
}

// Rows wraps SQLRows to allow struct slice scan
type Rows struct {
	sqlRows SQLRows
}

// Row wraps SQLRow to allow direct struct scan
type Row struct {
	sqlRow SQLRow
}

// ScanToStructSlice allows the scanning of sql rows to
// a struct slice
func (r *Rows) ScanToStructSlice(s interface{}) error {
	v := reflect.ValueOf(s)

	if v.Kind() != reflect.Ptr {
		return errors.New("not a pointer")
	}

	v = v.Elem()

	if v.Kind() != reflect.Slice {
		return errors.New("not a slice")
	}

	typ := v.Type()

	results := make(map[string][]interface{})

	cols, err := r.sqlRows.Columns()
	if err != nil {
		return err
	}

	var rows int
	for r.sqlRows.Next() {
		rows++
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := r.sqlRows.Scan(columnPointers...); err != nil {
			return err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			results[colName] = append(results[colName], *val)
		}
	}

	for i := 0; i < rows; i++ {
		strct := reflect.New(typ.Elem()).Elem()

		for j := 0; j < typ.Elem().NumField(); j++ {
			field := typ.Elem().Field(j).Tag.Get("sqlcol")
			if field == "-" {
				continue
			}

			value := results[field][i]

			strct.Field(j).Set(reflect.ValueOf(value))
		}

		v.Set(reflect.Append(v, strct))
	}

	return nil
}

// ScanToStruct allows the scanning of a row to a struct
func (r *Row) ScanToStruct(s interface{}) error {
	v := reflect.ValueOf(s)

	if v.Kind() != reflect.Ptr {
		return errors.New("not a pointer")
	}

	v = v.Elem()

	if v.Kind() != reflect.Struct {
		return errors.New("not a struct")
	}

	typ := v.Type()

	results := make(map[string]interface{})

	cols, err := r.sqlRow.Columns()
	if err != nil {
		return err
	}

	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := r.sqlRow.Scan(columnPointers...); err != nil {
		return err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		results[colName] = *val
	}

	strct := reflect.New(typ).Elem()

	for j := 0; j < typ.NumField(); j++ {
		field := typ.Field(j).Tag.Get("sqlcol")
		if field == "-" {
			continue
		}

		value := results[field]

		strct.Field(j).Set(reflect.ValueOf(value))
	}

	v.Set(strct)

	return nil
}

package sqlhelper

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type Tester struct {
	Likes    int    `sqlcol:"likes"`
	Dislikes int    `sqlcol:"dislikes"`
	Day      string `sqlcol:"day"`
}

type testSQLRows struct {
	iteration int
}

type testSQLRow struct{}

func (t *testSQLRows) Next() bool {
	return t.iteration == 1
}

func (t *testSQLRows) Columns() ([]string, error) {
	return []string{"likes", "dislikes", "day"}, nil
}

func (t *testSQLRow) Columns() ([]string, error) {
	return []string{"likes", "dislikes", "day"}, nil
}

func (t *testSQLRows) Close() error {
	return nil
}

func (t *testSQLRows) Scan(args ...interface{}) error {
	likes := args[0].(*interface{})
	dislikes := args[1].(*interface{})
	day := args[2].(*interface{})
	t.iteration++

	*likes = 5
	*dislikes = 2
	*day = "Monday"

	return nil
}

func (t *testSQLRow) Scan(args ...interface{}) error {
	likes := args[0].(*interface{})
	dislikes := args[1].(*interface{})
	day := args[2].(*interface{})

	*likes = 5
	*dislikes = 2
	*day = "Monday"

	return nil
}

func TestScannerToSlice(t *testing.T) {
	Convey("test rows scan to struct slice", t, func() {
		rows := Rows{&testSQLRows{1}}

		tester := []Tester{}

		err := rows.ScanToStructSlice(&tester)
		So(err, ShouldBeNil)
		So(len(tester), ShouldEqual, 1)
		So(tester[0].Likes, ShouldEqual, 5)
	})
}

func TestScannerToStruct(t *testing.T) {
	Convey("test row scans to struct", t, func() {
		row := Row{&testSQLRow{}}

		tester := Tester{}

		err := row.ScanToStruct(&tester)
		So(err, ShouldBeNil)
		So(tester.Likes, ShouldEqual, 5)
	})
}

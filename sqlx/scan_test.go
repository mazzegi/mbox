package sqlx

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/mazzegi/mbox/testx"
	_ "modernc.org/sqlite"
)

func setupDB(vals [][3]any) (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open memory db: %w", err)
	}
	_, err = db.Exec(`
		CREATE TABLE test (
			string 	TEXT,
			int	   	INTEGER,
			real    REAL			
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("exec create table: %w", err)
	}
	for _, vs := range vals {
		_, err = db.Exec(`INSERT INTO test (string,int,real) VALUES (?,?,?);`, vs[0], vs[1], vs[2])
		if err != nil {
			return nil, fmt.Errorf("exec insert: %w", err)
		}
	}
	return db, nil
}

func TestScan(t *testing.T) {
	tx := testx.NewTx(t)

	vals := [][3]any{}
	for i := 0; i < 10; i++ {
		vals = append(vals, [3]any{
			fmt.Sprintf("%04d", i+1), i + 1, float64(i+1) + 0.1,
		})
	}

	db, err := setupDB(vals)
	tx.AssertNoErr(err)
	defer db.Close()

	rows, err := db.Query(`SELECT string, int, real FROM test ORDER BY string ASC;`)
	tx.AssertNoErr(err)
	defer rows.Close()

	type testData struct {
		Str  string  `sql:"string"`
		Int  int     `sql:"int"`
		Real float64 `sql:"real"`
	}

	scanner, err := NewScanner(rows, nil)
	tx.AssertNoErr(err)
	i := 0
	for scanner.Next() {
		td, err := Scan[testData](scanner, rows)
		tx.AssertNoErr(err)
		tx.AssertEqual(testData{
			Str:  vals[i][0].(string),
			Int:  vals[i][1].(int),
			Real: vals[i][2].(float64),
		}, td)
		i++
	}
}

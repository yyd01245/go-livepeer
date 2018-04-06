package common

import (
	"database/sql"
	"fmt"
	"math/big"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func tempDB(t *testing.T) (*DB, *sql.DB, error) {
	dbpath := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	dbh, err := InitDB(dbpath)
	if err != nil {
		t.Error("Unable to initialize DB ", err)
		return nil, nil, err
	}
	raw, err := sql.Open("sqlite3", dbpath)
	if err != nil {
		t.Error("Unable to open raw sqlite db ", err)
		return nil, nil, err
	}
	return dbh, raw, nil
}

func TestDBLastSeenBlock(t *testing.T) {
	dbh, dbraw, err := tempDB(t)
	if err != nil {
		return
	}
	defer dbh.Close()
	defer dbraw.Close()

	// sanity check default value
	var val int64
	var created_at string
	var updated_at string
	stmt := "SELECT value, updated_at FROM kv WHERE key = 'lastBlock'"
	row := dbraw.QueryRow(stmt)
	err = row.Scan(&val, &created_at)
	if err != nil || val != int64(0) {
		t.Errorf("Unexpected result from sanity check; got %v - %v", err, val)
		return
	}
	// set last updated at timestamp to sometime in the past
	update_stmt := "UPDATE kv SET updated_at = datetime('now', '-2 months') WHERE key = 'lastBlock'"
	_, err = dbraw.Exec(update_stmt) // really should sanity check this result
	if err != nil {
		t.Error("Could not update db ", err)
	}

	// now test set
	blkval := int64(54321)
	err = dbh.SetLastSeenBlock(big.NewInt(blkval))
	if err != nil {
		t.Error("Unable to set last seen block ", err)
		return
	}
	row = dbraw.QueryRow(stmt)
	err = row.Scan(&val, &updated_at)
	if err != nil || val != blkval {
		t.Errorf("Unexpected result from value check; got %v - %v", err, val)
		return
	}
	// small possibility of a test failure if we executed over a 1s boundary
	if updated_at != created_at {
		t.Errorf("Unexpected result from update check; got %v:%v", updated_at, created_at)
		return
	}
}
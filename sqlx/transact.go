package sqlx

import (
	"database/sql"
	"fmt"
)

type TransactionStarter interface {
	Begin() (*sql.Tx, error)
}

// Transact encapsulates the call to fn into a transaction
// func Transact(db *sql.DB, fn func(tx *sql.Tx) error) error {
func Transact(db TransactionStarter, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}
	err = fn(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

package sqlitex

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func NewDB(file string) (*DB, error) {
	writer, err := setupWriter(file)
	if err != nil {
		return nil, fmt.Errorf("setup-writer: %w", err)
	}
	reader, err := setupReader(file)
	if err != nil {
		return nil, fmt.Errorf("setup-writer: %w", err)
	}
	return &DB{
		writer: writer,
		reader: reader,
	}, nil
}

type DB struct {
	writer *sql.DB
	reader *sql.DB
}

func setupWriter(file string) (*sql.DB, error) {
	params := strings.Join([]string{
		"_journal_mode=WAL",
		"_synchronous=NORMAL",
		"_busy_timeout=5000",
		"_txlock=immediate",
	}, "&")
	dsn := fmt.Sprintf("file:%s?%s", file, params)
	sdb, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", file, err)
	}
	sdb.SetMaxOpenConns(1)
	return sdb, nil
}

func setupReader(file string) (*sql.DB, error) {
	params := strings.Join([]string{
		"_busy_timeout=5000",
		"mode=ro",
	}, "&")
	dsn := fmt.Sprintf("file:%s?%s", file, params)
	sdb, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", file, err)
	}
	sdb.SetMaxOpenConns(5000)
	return sdb, nil
}

func (db *DB) Close() {
	db.reader.Close()
	db.writer.Close()
}

func (db *DB) Begin() (*sql.Tx, error) {
	return db.writer.BeginTx(context.Background(), nil)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.writer.BeginTx(ctx, opts)
}

func (db *DB) Exec(query string, args ...any) (sql.Result, error) {
	return db.writer.ExecContext(context.Background(), query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.writer.ExecContext(ctx, query, args...)
}

func (db *DB) Query(query string, args ...any) (*sql.Rows, error) {
	return db.reader.QueryContext(context.Background(), query, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.reader.QueryContext(ctx, query, args...)
}

func (db *DB) QueryRow(query string, args ...any) *sql.Row {
	return db.reader.QueryRowContext(context.Background(), query, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return db.reader.QueryRowContext(ctx, query, args...)
}

func (db *DB) PrepareExecContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.writer.PrepareContext(ctx, query)
}

func (db *DB) PrepareQueryContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.reader.PrepareContext(ctx, query)
}

func (db *DB) Stats() (writerStats, readerStats sql.DBStats) {
	return db.writer.Stats(), db.reader.Stats()
}

package es

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mazzegi/log"
	"github.com/mazzegi/mbox/slicesx"
	"github.com/mazzegi/mbox/sqlitex"
	"github.com/mazzegi/mbox/sqlx"
)

func NewSqliteXStore(file string) (*SqliteXStore, error) {
	db, err := sqlitex.NewDB(file)
	if err != nil {
		return nil, fmt.Errorf("new-db %q: %w", file, err)
	}

	s := &SqliteXStore{
		Hook:      log.ComponentHook("event-store"),
		publisher: NewStreamUpdatePublisher(),
		db:        db,
	}

	err = s.init()
	if err != nil {
		s.db.Close()
		return nil, fmt.Errorf("init: %w", err)
	}

	err = s.prepare()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("prepare: %w", err)
	}

	return s, nil
}

func (s *SqliteXStore) init() error {
	_, err := s.db.Exec(v1_init)
	if err != nil {
		return fmt.Errorf("exec v1_init: %w", err)
	}
	return nil
}

func (s *SqliteXStore) Close() {
	s.publisher.Close()
	s.statements.insertEvents.Close()
	s.db.Close()
}

type SqliteXStore struct {
	*log.Hook
	db         *sqlitex.DB
	publisher  *StreamUpdatePublisher
	statements statements
}

type statements struct {
	insertEvents *sql.Stmt
}

func (s *SqliteXStore) prepare() error {
	var err error
	s.statements.insertEvents, err = s.db.PrepareExecContext(context.Background(),
		"INSERT INTO events (id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data) VALUES(?,?,?,?,?,?,?,?);",
	)
	if err != nil {
		return fmt.Errorf("prepare insert events: %w", err)
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

func (s *SqliteXStore) Subscribe(streamID StreamID) *StreamUpdateSubscription {
	return s.publisher.Subscribe(streamID)
}

func (s *SqliteXStore) StreamVersion(streamID StreamID) uint64 {
	row := s.db.QueryRow("SELECT MAX(stream_index)+1 FROM events WHERE stream_id = ?;", streamID)
	var ver uint64
	err := row.Scan(&ver)
	if err != nil {
		return 0
	}
	return ver
}

func (s *SqliteXStore) StoreVersion() uint64 {
	row := s.db.QueryRow("SELECT MAX(store_index)+1 FROM events;")
	var ver uint64
	err := row.Scan(&ver)
	if err != nil {
		return 0
	}
	return ver
}

func (s *SqliteXStore) Append(streamID StreamID, expectedVersion uint64, events ...RawEvent) error {
	err := sqlx.Transact(s.db, func(tx *sql.Tx) error {
		streamVer := s.StreamVersion(streamID)
		if streamVer != expectedVersion {
			return NewExpectedVersionError(expectedVersion, streamVer)
		}

		storeVer := s.StoreVersion()
		for _, e := range events {
			_, err := tx.Stmt(s.statements.insertEvents).Exec(
				e.ID,
				storeVer,
				streamID,
				streamVer,
				formatTime(e.OccurredOn),
				formatTime(time.Now().UTC()),
				e.Type,
				string(e.Data),
			)
			if err != nil {
				return err
			}
			storeVer++
			streamVer++
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.publisher.PublishStreamUpdate(streamID)
	return nil
}

func (s *SqliteXStore) Create(events ...RawEvent) error {
	err := sqlx.Transact(s.db, func(tx *sql.Tx) error {
		storeVer := s.StoreVersion()
		for _, e := range events {
			streamVer := s.StreamVersion(StreamID(e.StreamID))
			_, err := tx.Stmt(s.statements.insertEvents).Exec(
				e.ID,
				storeVer,
				e.StreamID,
				streamVer,
				formatTime(e.OccurredOn),
				formatTime(time.Now().UTC()),
				e.Type,
				string(e.Data),
			)
			if err != nil {
				return err
			}
			storeVer++
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.publisher.PublishStreamUpdate(StreamIDAll)
	return nil
}

func (s *SqliteXStore) Find(id ID) (RawEvent, bool) {
	row := s.db.QueryRow(`
		SELECT 
			store_index, stream_id, stream_index, occurred_on, recorded_on, type, data 
		FROM events
		WHERE id = ?;
	`, string(id))

	var storeIndex uint64
	var streamID string
	var streamIndex uint64
	var occurredOn string
	var recordedOn string
	var typ string
	var data string
	err := row.Scan(&storeIndex, &streamID, &streamIndex, &occurredOn, &recordedOn, &typ, &data)
	if err != nil {
		return RawEvent{}, false
	}
	return RawEvent{
		ID:          ID(id),
		StoreIndex:  storeIndex,
		StreamID:    streamID,
		StreamIndex: streamIndex,
		OccurredOn:  parseTime(occurredOn),
		RecordedOn:  parseTime(recordedOn),
		Type:        typ,
		Data:        json.RawMessage(data),
	}, true
}

func (s *SqliteXStore) Query(params QueryParams, lo LimitOffset) (RawEvents, error) {
	var sort string
	if params.SortASC {
		sort = "ASC"
	} else {
		sort = "DESC"
	}

	var wheres []string
	args := []any{}
	if params.StreamID != string(StreamIDAll) && params.StreamID != "" {
		wheres = append(wheres, "stream_id = ?")
		args = append(args, params.StreamID)
	}
	if !params.ToDate.IsZero() {
		wheres = append(wheres, "occurred_on <= ?")
		args = append(args, params.ToDate)
	}
	if params.Type != "" {
		wheres = append(wheres, "type = ?")
		args = append(args, params.Type)
	}
	args = append(args, lo.Offset, lo.Limit)
	var where string
	if len(wheres) > 0 {
		where = "WHERE " + strings.Join(wheres, " AND ")
	}
	stmt := fmt.Sprintf(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data 
		FROM events %s ORDER BY store_index %s LIMIT ?,?;`, where, sort)

	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) QueryWithTypePrefix(prefix string, params QueryParams, lo LimitOffset) (RawEvents, error) {
	var sort string
	if params.SortASC {
		sort = "ASC"
	} else {
		sort = "DESC"
	}

	var wheres []string
	args := []any{}
	if params.StreamID != string(StreamIDAll) && params.StreamID != "" {
		wheres = append(wheres, "stream_id = ?")
		args = append(args, params.StreamID)
	}
	if !params.ToDate.IsZero() {
		wheres = append(wheres, "occurred_on <= ?")
		args = append(args, params.ToDate)
	}
	if params.Type != "" {
		wheres = append(wheres, "type = ?")
		args = append(args, params.Type)
	} else {
		wheres = append(wheres, fmt.Sprintf("type like '%s:%%'", prefix))
	}
	args = append(args, lo.Offset, lo.Limit)
	var where string
	if len(wheres) > 0 {
		where = "WHERE " + strings.Join(wheres, " AND ")
	}
	stmt := fmt.Sprintf(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data 
		FROM events %s ORDER BY store_index %s LIMIT ?,?;`, where, sort)

	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) LoadSlice(streamID StreamID, lo LimitOffset) (RawEvents, error) {
	var rows *sql.Rows
	var err error
	if streamID.IsAll() {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events ORDER BY store_index ASC LIMIT ?,?;`, lo.Offset, lo.Limit)
	} else {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events WHERE stream_id = ? ORDER BY stream_index ASC LIMIT ?,?;`, streamID, lo.Offset, lo.Limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) LoadSliceFromVersion(streamID StreamID, version uint64, lo LimitOffset) (RawEvents, error) {
	var rows *sql.Rows
	var err error
	if streamID.IsAll() {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events WHERE store_index >= ? ORDER BY store_index ASC LIMIT ?,?;`, version, lo.Offset, lo.Limit)
	} else {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events WHERE stream_index >= ? AND stream_id = ? ORDER BY stream_index ASC LIMIT ?,?;`, version, streamID, lo.Offset, lo.Limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) LoadSliceDescending(streamID StreamID, lo LimitOffset) (RawEvents, error) {
	var rows *sql.Rows
	var err error
	if streamID.IsAll() {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events ORDER BY store_index DESC LIMIT ?,?;`, lo.Offset, lo.Limit)
	} else {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events WHERE stream_id = ? ORDER BY stream_index DESC LIMIT ?,?;`, streamID, lo.Offset, lo.Limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) LoadSliceUntil(streamID StreamID, lo LimitOffset, until time.Time) (RawEvents, error) {
	var rows *sql.Rows
	var err error
	if streamID.IsAll() {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events WHERE occurred_on <= ? ORDER BY store_index ASC LIMIT ?,?;`, until, lo.Offset, lo.Limit)
	} else {
		rows, err = s.db.Query(`SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
			FROM events WHERE stream_id = ? AND occurred_on <= ? ORDER BY stream_index ASC LIMIT ?,?;`, streamID, until, lo.Offset, lo.Limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}
func (s *SqliteXStore) LoadLatestFromAll() (RawEvents, error) {
	rows, err := s.db.Query(`
		WITH msi AS ( SELECT stream_id, MAX(stream_index) AS max_stream_index FROM events GROUP BY stream_id )

		SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
		FROM events es
			INNER JOIN msi ON (stream_id = msi.stream_id AND stream_index = msi.max_stream_index );
	`)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) LoadLatestFrom(streamIDs []string) (RawEvents, error) {
	placeholders := strings.Join(slicesx.Repeat("?", len(streamIDs)), ",")
	rows, err := s.db.Query(fmt.Sprintf(`
		WITH msi AS ( SELECT stream_id, MAX(stream_index) AS max_stream_index FROM events WHERE stream_id IN (%s) GROUP BY stream_id )

		SELECT id, store_index, stream_id, stream_index, occurred_on, recorded_on, type, data
		FROM events es
		INNER JOIN msi ON (stream_id = msi.stream_id AND stream_index = msi.max_stream_index );
	`, placeholders), slicesx.Anys(streamIDs)...)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := RawEvents{}
	for rows.Next() {
		evt, err := s.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		res = append(res, evt)
	}
	return res, nil
}

func (s *SqliteXStore) scanEvent(rows *sql.Rows) (RawEvent, error) {
	var id string
	var storeIndex uint64
	var streamID string
	var streamIndex uint64
	var occurredOn string
	var recordedOn string
	var typ string
	var data string
	err := rows.Scan(&id, &storeIndex, &streamID, &streamIndex, &occurredOn, &recordedOn, &typ, &data)
	if err != nil {
		return RawEvent{}, err
	}
	return RawEvent{
		ID:          ID(id),
		StoreIndex:  storeIndex,
		StreamID:    streamID,
		StreamIndex: streamIndex,
		OccurredOn:  parseTime(occurredOn),
		RecordedOn:  parseTime(recordedOn),
		Type:        typ,
		Data:        json.RawMessage(data),
	}, nil
}

func (s *SqliteXStore) PurgeBefore(t time.Time) (numDeleted int, err error) {
	res, err := s.db.Exec(`DELETE FROM events WHERE recorded_on < ?;`, t.UTC())
	if err != nil {
		return 0, fmt.Errorf("exec-delete-before %q: %w", formatTime(t.UTC()), err)
	}
	aff, _ := res.RowsAffected()
	return int(aff), nil
}

func (s *SqliteXStore) AllStreamIDs() ([]StreamID, error) {
	rows, err := s.db.Query(`SELECT DISTINCT stream_id FROM events;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []StreamID{}
	var streamID sql.NullString
	for rows.Next() {
		err := rows.Scan(&streamID)
		if err != nil {
			return nil, err
		}
		res = append(res, StreamID(streamID.String))
	}
	return res, nil
}

const v1_init = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous = OFF;

CREATE TABLE IF NOT EXISTS events (
	id				TEXT,
	store_index		INTEGER,
	stream_id		TEXT,
	stream_index	INTEGER,
	occurred_on		TEXT,
	recorded_on		TEXT,
	type 			TEXT,
	data			TEXT,
	PRIMARY KEY (store_index)	
);

CREATE INDEX IF NOT EXISTS idx_events_stream
ON events (stream_id, stream_index);
`

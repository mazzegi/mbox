package entity_v3

import (
	"fmt"
	"strings"

	"github.com/mazzegi/mbox/blobix_v2"
	"github.com/mazzegi/mbox/es"
	"github.com/mazzegi/mbox/makex"
	"github.com/mazzegi/mbox/query"
	"github.com/mazzegi/mbox/urn"

	"github.com/r3labs/diff/v3"
)

type Entity interface {
	EntityID() string
}

type Created[T Entity] struct {
	es.Base
	Entity T `json:"entity"`
}

type Deleted[T Entity] struct {
	es.Base
	EntityID string `json:"entity-id"`
}

type Changed[T Entity] struct {
	es.Base
	EntityID  string         `json:"entity-id"`
	Changelog diff.Changelog `json:"changelog"`
}

// when the entity model changes, replaced is used
type Replaced[T Entity] struct {
	es.Base
	Entity T `json:"entity"`
}

type UpdateAction string

const (
	UpdateActionNone    UpdateAction = "none"
	UpdateActionCreate  UpdateAction = "create"
	UpdateActionChange  UpdateAction = "change"
	UpdateActionDelete  UpdateAction = "delete"
	UpdateActionReplace UpdateAction = "replace"
)

type UpdateResult struct {
	ID           string
	Action       UpdateAction
	Diff         diff.Changelog
	Version      uint64
	StoreVersion uint64
}

type Blob[T Entity] struct {
	EntityID      string `json:"entity-id"`
	StreamID      string `json:"stream-id"`
	StreamVersion uint64 `json:"stream-version"`
	Deleted       bool   `json:"deleted"`
	Data          T      `json:"data"`
}

//

func NewStore[T Entity](prefix string, events es.Store, snapQueries blobix_v2.Store) *Store[T] {
	codec := es.NewCodec()
	codec.Register(urn.Make(prefix, "created").String(), Created[T]{})
	codec.Register(urn.Make(prefix, "deleted").String(), Deleted[T]{})
	codec.Register(urn.Make(prefix, "changed").String(), Changed[T]{})
	codec.Register(urn.Make(prefix, "replaced").String(), Replaced[T]{})

	snapBucket := blobix_v2.NewBucket[Blob[T]](snapQueries, prefix)
	return &Store[T]{
		prefix:      prefix,
		events:      events,
		snapQueries: snapQueries,
		snapBucket:  *snapBucket,
		codec:       codec,
	}
}

type Store[T Entity] struct {
	prefix      string
	events      es.Store
	snapQueries blobix_v2.Store
	snapBucket  blobix_v2.Bucket[Blob[T]]
	codec       *es.Codec
}

func (s *Store[T]) Codec() *es.Codec {
	return s.codec
}

func (s *Store[T]) StreamID(entityID string) es.StreamID {
	return es.StreamID(urn.Make(s.prefix, entityID).String())
}

func (s *Store[T]) EntityID(streamID es.StreamID) string {
	return strings.TrimPrefix(string(streamID), s.prefix+":")
}

func (s *Store[T]) StoreVersion() uint64 {
	return s.events.StoreVersion()
}

func (s *Store[T]) Load(entityID string) (T, uint64, query.Found, error) {
	bl, found, err := s.snapBucket.Find(entityID)
	if err != nil {
		return makex.ZeroOf[T](), 0, false, fmt.Errorf("snap-bucket.find: %w", err)
	}
	if !found {
		return makex.ZeroOf[T](), 0, false, nil
	}
	if bl.Deleted {
		return makex.ZeroOf[T](), 0, false, nil
	}
	return bl.Data, bl.StreamVersion, true, nil
}

func (s *Store[T]) LoadBlob(entityID string) (Blob[T], query.Found, error) {
	bl, found, err := s.snapBucket.Find(entityID)
	if err != nil {
		return makex.ZeroOf[Blob[T]](), false, fmt.Errorf("snap-bucket.find: %w", err)
	}
	if !found {
		return makex.ZeroOf[Blob[T]](), false, nil
	}
	if bl.Deleted {
		return makex.ZeroOf[Blob[T]](), false, nil
	}
	return bl, true, nil
}

func (s *Store[T]) Diff(old, new T) (diff.Changelog, error) {
	cl, err := diff.Diff(old, new)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}
	return cl, nil
}

func (s *Store[T]) Update(newEnt T, oldEnt T, ver uint64, meta es.MetaData) (UpdateResult, error) {
	entityID := oldEnt.EntityID()
	newEntityID := newEnt.EntityID()
	if entityID != newEntityID {
		return UpdateResult{}, fmt.Errorf("old and new have different ids: old=%q; new=%q", entityID, newEntityID)
	}
	changelog, err := diff.Diff(oldEnt, newEnt)
	if err != nil {
		// diff failed, this may happen due to model changes - try to replace existing entity
		return s.replace(newEnt, ver, meta)
	}
	if len(changelog) == 0 {
		//-- entity may not have changed but maybe deleted and created - check this
		return UpdateResult{
			ID:           entityID,
			Action:       UpdateActionNone,
			Version:      ver,
			StoreVersion: s.events.StoreVersion(),
		}, nil
	}

	e := Changed[T]{
		Base:      es.MakeBaseWithMeta(meta),
		EntityID:  entityID,
		Changelog: changelog,
	}
	es.WithMeta(e, meta)

	re, err := s.codec.Encode(e)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("encode changed event: %w", err)
	}
	err = s.events.Append(s.StreamID(entityID), ver, re)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("append changed event: %w", err)
	}
	newVersion := ver + 1

	//snapshot
	err = s.snapBucket.Save(entityID, Blob[T]{
		EntityID:      entityID,
		StreamID:      string(s.StreamID(entityID)),
		StreamVersion: newVersion,
		Deleted:       false,
		Data:          newEnt,
	})

	if err != nil {
		return UpdateResult{}, fmt.Errorf("save snapshot")
	}
	return UpdateResult{
		ID:           entityID,
		Action:       UpdateActionChange,
		Diff:         changelog,
		Version:      newVersion,
		StoreVersion: s.events.StoreVersion(),
	}, nil
}

func (s *Store[T]) Save(ent T, meta es.MetaData) (UpdateResult, error) {
	entityID := ent.EntityID()
	//currEnt, ver, deleted, err := s.loadBlob(entityID)
	bl, found, err := s.LoadBlob(entityID)
	switch {
	case bl.Deleted:
		// reincarnated !!
		return s.replace(ent, bl.StreamVersion, meta)
	case !bool(found) || bl.StreamVersion == 0:
		return s.Create(ent, meta)
	case err != nil:
		return UpdateResult{}, fmt.Errorf("load %q: %w", entityID, err)
	}
	return s.Update(ent, bl.Data, bl.StreamVersion, meta)
}

func (s *Store[T]) Delete(entityID string, meta es.MetaData) (UpdateResult, error) {
	ent, ver, found, err := s.Load(entityID)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("load %q: %w", entityID, err)
	}
	if !found {
		return UpdateResult{}, fmt.Errorf("not found %q", entityID)
	}

	//
	e := Deleted[T]{
		Base:     es.MakeBaseWithMeta(meta),
		EntityID: entityID,
	}
	re, err := s.codec.Encode(e)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("encode delete event: %w", err)
	}
	err = s.events.Append(s.StreamID(entityID), ver, re)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("append delete event: %w", err)
	}
	newVersion := ver + 1

	//snapshot
	err = s.snapBucket.Save(entityID, Blob[T]{
		EntityID:      entityID,
		StreamID:      string(s.StreamID(entityID)),
		StreamVersion: newVersion,
		Deleted:       true,
		Data:          ent,
	})

	if err != nil {
		return UpdateResult{}, fmt.Errorf("save snapshot")
	}
	return UpdateResult{
		ID:           entityID,
		Action:       UpdateActionDelete,
		Version:      newVersion,
		StoreVersion: s.events.StoreVersion(),
	}, nil
}

func (s *Store[T]) Create(ent T, meta es.MetaData) (UpdateResult, error) {
	e := Created[T]{
		Base:   es.MakeBaseWithMeta(meta),
		Entity: ent,
	}
	re, err := s.codec.Encode(e)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("encode created event: %w", err)
	}
	entityID := ent.EntityID()
	err = s.events.Append(s.StreamID(entityID), 0, re)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("append created event: %w", err)
	}
	// update snapshots
	err = s.snapBucket.Save(entityID, Blob[T]{
		EntityID:      entityID,
		StreamID:      string(s.StreamID(entityID)),
		StreamVersion: 1,
		Deleted:       false,
		Data:          ent,
	})
	if err != nil {
		return UpdateResult{}, fmt.Errorf("save snapshot")
	}

	return UpdateResult{
		ID:           entityID,
		Action:       UpdateActionCreate,
		Version:      1,
		StoreVersion: s.events.StoreVersion(),
	}, nil
}

func (s *Store[T]) replace(ent T, ver uint64, meta es.MetaData) (UpdateResult, error) {
	e := Replaced[T]{
		Base:   es.MakeBaseWithMeta(meta),
		Entity: ent,
	}
	re, err := s.codec.Encode(e)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("encode replaced event: %w", err)
	}
	entityID := ent.EntityID()
	err = s.events.Append(s.StreamID(entityID), ver, re)
	if err != nil {
		return UpdateResult{}, fmt.Errorf("append replaced event: %w", err)
	}
	newVersion := ver + 1

	// update snapshots
	err = s.snapBucket.Save(entityID, Blob[T]{
		EntityID:      entityID,
		StreamID:      string(s.StreamID(entityID)),
		StreamVersion: newVersion,
		Deleted:       false,
		Data:          ent,
	})
	if err != nil {
		return UpdateResult{}, fmt.Errorf("save snapshot")
	}

	return UpdateResult{
		ID:           entityID,
		Action:       UpdateActionReplace,
		Version:      newVersion,
		StoreVersion: s.events.StoreVersion(),
	}, nil
}

type EntityPage[T Entity] struct {
	Entities []T
	Error    error
	Idx      int
}

func (s *Store[T]) StreamEntities(pageLimit int) <-chan EntityPage[T] {
	c := make(chan EntityPage[T])
	go func() {
		defer close(c)
		for kp := range blobix_v2.StreamKeys(s.snapQueries, s.prefix, pageLimit) {
			if kp.Error != nil {
				c <- EntityPage[T]{Error: kp.Error}
				return
			}
			ep := EntityPage[T]{
				Entities: make([]T, len(kp.Keys)),
				Idx:      kp.Idx,
			}
			for i, key := range kp.Keys {
				b, found, err := s.snapBucket.Find(key)
				if err != nil {
					c <- EntityPage[T]{Error: err}
					return
				}
				if !found {
					c <- EntityPage[T]{Error: fmt.Errorf("no found %q", key)}
					return
				}
				if b.Deleted {
					continue
				}
				ep.Entities[i] = b.Data
			}
			c <- ep
		}
	}()
	return c
}

func (s *Store[T]) Query(params es.QueryParams, lo es.LimitOffset) (es.RawEvents, error) {
	return s.events.QueryWithTypePrefix(s.prefix, params, lo)
}

func (s *Store[T]) EventsSince(ver uint64, limit int) (es.RawEvents, error) {
	rawEs, err := s.events.LoadSlice(es.StreamIDAll, es.LimitOffset{Offset: ver, Limit: uint64(limit)})
	if err != nil {
		return nil, fmt.Errorf("events.load-slice: %w", err)
	}
	return rawEs, nil
}

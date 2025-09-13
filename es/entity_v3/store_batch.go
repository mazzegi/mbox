package entity_v3

import (
	"fmt"

	"github.com/mazzegi/mbox/blobix_v2"
	"github.com/mazzegi/mbox/es"
	"github.com/mazzegi/mbox/slicesx"
	"github.com/r3labs/diff/v3"
)

func (s *Store[T]) SaveMany(ents []T, meta es.MetaData) ([]UpdateResult, error) {

	// ids/keys
	keys := slicesx.Map(ents, func(t T) string {
		return t.EntityID()
	})

	// load entity blobs
	//bucket := s.snapQueries.Bucket(s.prefix)
	//blobs, err := blobix.Values[Blob[T]](bucket, keys...)
	blobs, err := s.snapBucket.Values(keys...)
	if err != nil {
		return nil, fmt.Errorf("blob.values: %w", err)
	}
	blobMap := map[string]Blob[T]{}
	for _, blob := range blobs {
		blobMap[blob.EntityID] = blob
	}

	// iterate through passed enntities
	var results []UpdateResult
	var rawEvents []es.RawEvent

	var newBlobs []blobix_v2.Tuple[string, Blob[T]]
	mkBlobTuple := func(key string, blob Blob[T]) blobix_v2.Tuple[string, Blob[T]] {
		return blobix_v2.MkTuple(key, blob)
	}

	// log.Debugf("store: generate diff and events ...")
	for _, ent := range ents {
		entID := ent.EntityID()
		streamID := string(s.StreamID(entID))
		blob, ok := blobMap[entID]
		if !ok {
			// create
			re, err := s.codec.Encode(Created[T]{
				Base:   es.MakeBaseWithMeta(meta),
				Entity: ent,
			})
			if err != nil {
				return nil, fmt.Errorf("encode create event: %w", err)
			}
			re.StreamID = streamID
			rawEvents = append(rawEvents, re)
			newBlobs = append(newBlobs, mkBlobTuple(entID, Blob[T]{
				EntityID:      entID,
				StreamID:      streamID,
				StreamVersion: 1,
				Deleted:       false,
				Data:          ent,
			}))
		} else {
			// update
			changelog, err := diff.Diff(blob.Data, ent)
			if err != nil {
				// // diff failed, this may happen due to model changes - try to replace existing entity
				re, err := s.codec.Encode(Replaced[T]{
					Base:   es.MakeBaseWithMeta(meta),
					Entity: ent,
				})
				if err != nil {
					return nil, fmt.Errorf("encode replaced event: %w", err)
				}
				re.StreamID = streamID
				rawEvents = append(rawEvents, re)
				newBlobs = append(newBlobs, mkBlobTuple(entID, Blob[T]{
					EntityID:      entID,
					StreamID:      streamID,
					StreamVersion: blob.StreamVersion + 1,
					Deleted:       false,
					Data:          ent,
				}))
			} else if len(changelog) == 0 {
				// no changes
			} else {
				// some changes
				re, err := s.codec.Encode(Changed[T]{
					Base:      es.MakeBaseWithMeta(meta),
					EntityID:  entID,
					Changelog: changelog,
				})
				if err != nil {
					return nil, fmt.Errorf("encode replaced event: %w", err)
				}
				re.StreamID = streamID
				rawEvents = append(rawEvents, re)
				newBlobs = append(newBlobs, mkBlobTuple(entID, Blob[T]{
					EntityID:      entID,
					StreamID:      streamID,
					StreamVersion: blob.StreamVersion + 1,
					Deleted:       false,
					Data:          ent,
				}))
			}
		}
	}
	if len(rawEvents) == 0 {
		// no updates
		return results, nil
	}

	// append events
	err = s.events.Create(rawEvents...)
	if err != nil {
		return nil, fmt.Errorf("create-events: %w", err)
	}

	// update snapshots
	//err = bucket.PutJSONMany(newBlobs...)
	err = s.snapBucket.SaveMany(newBlobs)
	if err != nil {
		return nil, fmt.Errorf("snapshots-put-many: %w", err)
	}

	return results, nil
}

func (s *Store[T]) LoadMany(entityIDs []string) ([]T, error) {
	//bucket := s.snapQueries.Bucket(s.prefix)
	//blobs, err := blobix.Values[Blob[T]](bucket, entityIDs...)
	blobs, err := s.snapBucket.Values(entityIDs...)
	if err != nil {
		return nil, fmt.Errorf("blob.values: %w", err)
	}
	ts := make([]T, 0, len(blobs))
	for _, blob := range blobs {
		if blob.Deleted {
			continue
		}
		ts = append(ts, blob.Data)
	}
	return ts, nil
}

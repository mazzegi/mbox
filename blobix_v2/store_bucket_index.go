package blobix_v2

import (
	"fmt"

	"github.com/mazzegi/log"
	"github.com/mazzegi/mbox/slicesx"
)

func (b Bucket[T]) AddOrUpdateIndex(idxName string, fields ...IndexField[T]) error {
	fieldDescs := slicesx.Map(fields, func(field IndexField[T]) IndexFieldDescriptor { return field.Descriptor })

	existingIdx, ok := b.store.FindIndexDescriptor(b.name, idxName)
	if !ok {
		err := b.store.CreateIndex(b.name, idxName, fieldDescs)
		if err != nil {
			return fmt.Errorf("store.create-index %q: %w", idxName, err)
		}
		err = b.updateAllIndexValues(idxName, fields...)
		if err != nil {
			return fmt.Errorf("pupdate-all-index-values: %w", err)
		}
		b.indexes[idxName] = BucketIndex[T]{
			IndexName: idxName,
			Fields:    fields,
		}
		return nil
	}
	newIdx := IndexDescriptor{
		BucketName: b.name,
		IndexName:  idxName,
		Fields:     fieldDescs,
	}
	if IndexDescriptorsEqual(existingIdx, newIdx) {
		// nothing to todo
		return nil
	}
	// ok - something changed - drop existing, create new, rebuild
	err := b.store.DeleteIndex(b.name, idxName)
	if err != nil {
		return fmt.Errorf("store.delete-index %q: %w", idxName, err)
	}
	err = b.store.CreateIndex(b.name, idxName, fieldDescs)
	if err != nil {
		return fmt.Errorf("store.create-index %q: %w", idxName, err)
	}
	b.indexes[idxName] = BucketIndex[T]{
		IndexName: idxName,
		Fields:    fields,
	}
	// ... and rebuild
	err = b.updateAllIndexValues(idxName, fields...)
	if err != nil {
		return fmt.Errorf("pupdate-all-index-values: %w", err)
	}

	return nil
}

func (b Bucket[T]) updateAllIndexValues(idxName string, fields ...IndexField[T]) error {
	tx, err := b.store.BeginTx()
	if err != nil {
		return fmt.Errorf("begin-tx: %w", err)
	}
	for kp := range StreamKeys(b.store, b.name, 500) {
		if kp.Error != nil {
			tx.Rollback()
			return fmt.Errorf("stream-keys: %w", kp.Error)
		}
		kvs, err := b.KeyValues(kp.Keys...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("key-values: %w", err)
		}

		log.Debugf("rebuild-index: page %d (%d keys)", kp.Idx+1, len(kp.Keys))
		for key, val := range kvs {
			values := indexValues(fields, val)
			err = tx.UpdateIndex(b.name, idxName, key, values)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("update-index for key %q: %w", key, err)
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

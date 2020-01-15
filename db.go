package badgers

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger"
)

type DB struct {
	db *badger.DB
}

// New a DB with path.
func New(path string) (*DB, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, fmt.Errorf("new db error: %w", err)
	}
	return &DB{db}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Server() *Server {
	return NewServer(db)
}

var ErrKeyNotFound = errors.New("key not found")

// Get by key.
func (db *DB) Get(key string) ([]byte, error) {
	var val []byte
	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, fmt.Errorf("get '%v' error: %w", key, err)
	}

	return val, nil
}

// Set kv.
func (db *DB) Set(key string, val []byte) error {
	if err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), val)
	}); err != nil {
		return fmt.Errorf("set '%s' error: %w", key, err)
	}
	return nil
}

// Delete by key.
func (db *DB) Delete(key string) error {
	if err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	}); err != nil {
		return fmt.Errorf("delete '%v' error: %w", key, err)
	}
	return nil
}

// ListKeys with prefix.
func (db *DB) ListKeys(prefix string) ([]string, error) {
	var keys []string
	err := db.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		p := []byte(prefix)
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			k := it.Item().KeyCopy(nil)
			keys = append(keys, string(k))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list keys error: %w", err)
	}
	return keys, nil
}

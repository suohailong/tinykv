package standalone_storage

import (
	"time"

	"github.com/boltdb/bolt"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db          *bolt.DB
	CfDefaultDB string
	CfLockDB    string
	CfWriteDB   string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	s := &StandAloneStorage{}
	// Your Code Here (1).
	db, err := bolt.Open(conf.DBPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalf("bolt db begin txn error: [%v]", err)
	}
	s.db = db
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return s.db.Update(func(t *bolt.Tx) error {
		_, err := t.CreateBucketIfNotExists([]byte(engine_util.CfDefault))
		_, err = t.CreateBucketIfNotExists([]byte(engine_util.CfLock))
		_, err = t.CreateBucketIfNotExists([]byte(engine_util.CfWrite))
		return err
	})
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	var writeError error
	for _, b := range batch {
		switch data := b.Data.(type) {
		case storage.Put:
			bucket := txn.Bucket([]byte(data.Cf))
			err = bucket.Put(b.Key(), b.Value())
			if err != nil {
				writeError = err
				break
			}
		case storage.Delete:
			bucket := txn.Bucket([]byte(data.Cf))
			err = bucket.Delete(b.Key())
			if err != nil {
				writeError = err
				break
			}
		}
	}
	if writeError != nil {
		txn.Rollback()
	}
	txn.Commit()
	return err
}

/****
iterator
***/
type StandAloneReader struct {
	StandAloneStorage
}

// When the key doesn't exist, return nil for the value
func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	var (
		value []byte
		err   error
	)
	err = s.db.View(func(t *bolt.Tx) error {
		b := t.Bucket([]byte(cf))
		value = b.Get(key)
		return nil
	})
	return value, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {

	return &StandAloneIterator{}
}

func (s *StandAloneReader) Close() {}

/****
iterator
***/

type StandAloneIterator struct {
	txn    *bolt.Tx
	cursor *bolt.Cursor
}

func NewStandAloneIterator(db *bolt.DB, cf string) *StandAloneIterator {
	txn, _ := db.Begin(false)
	b := txn.Bucket([]byte(cf))

	cursor := b.Cursor()
	return &StandAloneIterator{
		txn:    txn,
		cursor: cursor,
	}

}

// Item returns pointer to the current key-value pair.
func (s *StandAloneIterator) Item() engine_util.DBItem {
	return &StandAloneItem{}
}

// Valid returns false when iteration is done.
func (s *StandAloneIterator) Valid() bool {
	return true
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (s *StandAloneIterator) Next() {
	s.cursor.Next()
}

// Seek would seek to the provided key if present. If absent, it would seek to the next smallest key
// greater than provided.
func (s *StandAloneIterator) Seek([]byte) {}

// Close the iterator
func (s *StandAloneIterator) Close() {}

/****
item
***/
type StandAloneItem struct{}

func (s *StandAloneItem) Key() []byte {
	return []byte{}
}

// KeyCopy returns a copy of the key of the item, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (s *StandAloneItem) KeyCopy(dst []byte) []byte {
	return []byte{}
}

// Value retrieves the value of the item.
func (s *StandAloneItem) Value() ([]byte, error) {
	return []byte{}, nil
}

// ValueSize returns the size of the value.
func (s *StandAloneItem) ValueSize() int {
	return 0
}

// ValueCopy returns a copy of the value of the item from the value log, writing it to dst slice.
// If nil is passed, or capacity of dst isn't sufficient, a new slice would be allocated and
// returned.
func (s *StandAloneItem) ValueCopy(dst []byte) ([]byte, error) {
	return []byte{}, nil
}

package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey []byte
	txn      *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		startKey: startKey,
		txn:      txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.txn.Reader.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is , then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.txn.Reader.IterCF(engine_util.CfWrite)
	defer iter.Close()
	iter.Seek(EncodeKey(scan.startKey, scan.txn.StartTS))
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		userKey := DecodeUserKey(item.Key())
		if bytes.Equal(key, userKey) {
			value, err := item.Value()
			if err != nil {
				log.Errorf("")
				return nil, err
			}
			write, err := ParseWrite(value)
			if err != nil {
				log.Errorf("")
				return nil, err
			}
			if write.Kind == WriteKindPut {
				return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			}
		}
	}
	return nil, nil, nil
}

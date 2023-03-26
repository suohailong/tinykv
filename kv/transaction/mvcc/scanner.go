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
	iter engine_util.DBIterator
	next []byte
	txn  *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		iter: iter,
		next: startKey,
		txn:  txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is , then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	// defer iter.Close()
	if scan.next == nil {
		return nil, nil, nil
	}

	key := scan.next
	scan.iter.Seek(EncodeKey(key, scan.txn.StartTS))
	if !scan.iter.Valid() {
		scan.next = nil
		return nil, nil, nil
	}

	// 获取当前的最新的值
	item := scan.iter.Item()
	currentKey := DecodeUserKey(item.Key())
	currentTs := decodeTimestamp(item.Key())
	for scan.iter.Valid() && currentTs > scan.txn.StartTS {
		scan.iter.Seek(EncodeKey(currentKey, scan.txn.StartTS))
		item := scan.iter.Item()
		currentKey = DecodeUserKey(item.Key())
		currentTs = decodeTimestamp(item.Key())
	}

	if !scan.iter.Valid() {
		scan.next = nil
		return nil, nil, nil

	}

	// 找到下一个key
	for ; scan.iter.Valid(); scan.iter.Next() {
		nextkey := DecodeUserKey(scan.iter.Item().Key())
		if !bytes.Equal(nextkey, currentKey) {
			scan.next = nextkey
			break
		}
	}

	if !scan.iter.Valid() {
		scan.next = nil
	}

	// if bytes.Equal(item.Key(), userKey) {
	value, err := item.Value()
	if err != nil {
		log.Errorf("")
		return currentKey, nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		log.Errorf("")
		return currentKey, nil, err
	}
	if write.Kind == WriteKindPut {
		v, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(item.Key(), write.StartTS))
		return currentKey, v, err
	}
	// }
	return currentKey, nil, nil
}

package standalone_storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type standAloneStorageTestSuite struct {
	suite.Suite
	storage *StandAloneStorage
}

func TestLanch(t *testing.T) {
	suite.Run(t, new(standAloneStorageTestSuite))
}

func (s *standAloneStorageTestSuite) SetupTest() {
	fmt.Println("start standAloneStorage")
	s.storage = NewStandAloneStorage(&config.Config{
		DBPath: "testdata/boltdb/standalone",
	})
	s.storage.Start()
}
func (s *standAloneStorageTestSuite) TearDownTest() {
	s.storage.Stop()
	os.RemoveAll("testdata/boltdb/standalone")
	fmt.Println("stop standAloneStorage")
}

func (s *standAloneStorageTestSuite) TestWrite() {
	ctx := &kvrpcpb.Context{}
	request := []storage.Modify{}
	for i := 0; i < 10; i++ {
		request = append(request, storage.Modify{Data: storage.Put{
			Key:   []byte("ni@" + fmt.Sprint(i)),
			Value: []byte("ta@" + fmt.Sprint(i)),
			Cf:    "default",
		}})
	}

	err := s.storage.Write(ctx, request)
	fmt.Printf("storage write err: %v\n", err)

	s.storage.db.View(func(txn *bolt.Tx) error {
		b := txn.Bucket([]byte("default"))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})
}

func (s *standAloneStorageTestSuite) TestReader() {
	ctx := &kvrpcpb.Context{}
	request := []storage.Modify{}
	for i := 0; i < 10; i++ {
		request = append(request, storage.Modify{Data: storage.Put{
			Key:   []byte("ni@" + fmt.Sprint(i)),
			Value: []byte("ta@" + fmt.Sprint(i)),
			Cf:    "default",
		}})
	}

	err := s.storage.Write(ctx, request)
	fmt.Printf("storage write err: %v\n", err)

	ctx1 := &kvrpcpb.Context{}
	r, _ := s.storage.Reader(ctx1)
	value, err := r.GetCF("default", []byte("ni@1"))
	require.NoError(s.T(), err)
	s.T().Log("value:", string(value))

	iter := r.IterCF("default")

	for iter.Valid() {
		v, _ := iter.Item().Value()
		s.T().Logf("current, key: %s, value: %s", string(iter.Item().Key()), string(v))
		iter.Next()
		s.storage.Write(ctx1, []storage.Modify{
			{
				Data: storage.Put{
					Key:   []byte("wo"),
					Value: []byte("ta"),
					Cf:    "default",
				},
			},
		})
		fmt.Println("写入")
	}
	iter.Close()

}

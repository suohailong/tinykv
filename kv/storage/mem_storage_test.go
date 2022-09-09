package storage

// Basic imports
import (
	"fmt"
	"testing"

	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type MemoryStorageTestSuite struct {
	suite.Suite
	storage *MemStorage
}

func TestLanch(t *testing.T) {
	suite.Run(t, new(MemoryStorageTestSuite))
}

func (s *MemoryStorageTestSuite) SetupTest() {
	s.storage = NewMemStorage()
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (s *MemoryStorageTestSuite) TestWrite() {
	ctx := &kvrpcpb.Context{}

	request := []Modify{}
	for i := 0; i < 10; i++ {
		request = append(request, Modify{Data: Put{
			Key:   []byte("ni@" + fmt.Sprint(i)),
			Value: []byte("ta@" + fmt.Sprint(i)),
			Cf:    "default",
		}})
	}
	s.T().Logf("request: %v", request)
	err := s.storage.Write(ctx, request)
	s.T().Log(err)
	require.NoError(s.T(), err)

	r, err := s.storage.Reader(ctx)
	require.NoError(s.T(), err)

	iter := r.IterCF("default")
	for iter.Valid() {
		s.T().Log(string(iter.Item().Key()))
		value, err := iter.Item().Value()
		s.T().Log(string(value), err)
		iter.Next()
	}

}

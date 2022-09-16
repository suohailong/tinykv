package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	r, _ := server.storage.Reader(req.Context)
	v, err := r.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
			Value: v,
		}, err

	}

	if len(v) == 0 {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, err
	}

	return &kvrpcpb.RawGetResponse{
		Value: v,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.GetKey(),
				Value: req.GetValue(),
				Cf:    req.GetCf(),
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.GetKey(),
				Cf:  req.GetCf(),
			},
		},
	})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	result := []*kvrpcpb.KvPair{}
	r, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Kvs: result,
		}, err
	}
	iter := r.IterCF(req.GetCf())
	for iter.Valid() {
		if uint32(len(result)) >= req.GetLimit() {
			break
		}
		v, err := iter.Item().Value()
		if err != nil {
			result = append(result, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Abort: err.Error(),
				},
				Key:   iter.Item().Key(),
				Value: v,
			})

		} else {
			result = append(result, &kvrpcpb.KvPair{
				Key:   iter.Item().Key(),
				Value: v,
			})

		}
		iter.Next()
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: result,
	}, nil
}

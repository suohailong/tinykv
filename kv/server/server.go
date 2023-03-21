package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if rangeErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{
				RegionError: rangeErr.RequestErr,
			}, nil
		}
		return nil, err

	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// 检查一下有没有锁的存在
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		if rangeErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{
				RegionError: rangeErr.RequestErr,
			}, nil
		}
		return nil, err
	}
	// 如果存在锁,或者锁是在读之前加的
	if lock != nil || req.Version > lock.Ts {
		return &kvrpcpb.GetResponse{
			Error: &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         req.GetKey(),
					LockTtl:     lock.Ttl,
				},
			},
		}, nil
	}
	// 获取值
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		if rangeErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.GetResponse{
				RegionError: rangeErr.RequestErr,
			}, nil
		}
		return nil, err
	}

	return &kvrpcpb.GetResponse{
		Value: value,
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if rangeErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{
				RegionError: rangeErr.RequestErr,
			}, nil
		}
		return nil, err

	}
	keyErrors := make([]*kvrpcpb.KeyError, len(req.Mutations))
	defer reader.Close()
	//FIXME: 这里为什么不给所有的key加锁，防止多线程并发
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	// 1. 先检查一下当前事务开始的这个时间有没有新事物提交
	for _, mu := range req.Mutations {
		// FIXME: 这里为什么key没有添加开始时间戳
		write, writeTm, err := txn.MostRecentWrite(mu.GetKey())
		if err != nil {
			if rangeErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.PrewriteResponse{
					RegionError: rangeErr.RequestErr,
				}, nil
			}
			return nil, err
		}
		if write != nil && writeTm > req.GetStartVersion() {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: writeTm,
					Key:        mu.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		// 2. 在检查一下当前事务开始的这个时间有没有锁存在
		lock, err := txn.GetLock(mu.Key)
		if err != nil {
			if rangeErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.PrewriteResponse{
					RegionError: rangeErr.RequestErr,
				}, nil
			}
			return nil, err
		}
		if lock != nil {
			keyErrors = append(keyErrors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         mu.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}
		// 3. 写入lock和值,缓存到事务中
		switch mu.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mu.Key, mu.Value)
			txn.PutLock(mu.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindPut,
			})
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mu.Key)
			txn.PutLock(mu.Key, &mvcc.Lock{
				Primary: req.PrimaryLock,
				Ts:      req.StartVersion,
				Ttl:     req.LockTtl,
				Kind:    mvcc.WriteKindDelete,
			})
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.PrewriteResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		return nil, err
	}

	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if rangeErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{
				RegionError: rangeErr.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()
	// 给所有的key加锁，防止多线程并发
	defer server.Latches.ReleaseLatches(req.Keys)
	// 注意这里是开始时间
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	// 1. 查找锁
	for _, key := range req.GetKeys() {
		lock, err := txn.GetLock(key)
		if err != nil {
			if rangeErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.CommitResponse{
					RegionError: rangeErr.RequestErr,
				}, nil
			}
			return nil, err
		}
		if lock == nil {
			return &kvrpcpb.CommitResponse{}, nil
		}
		if lock.Ts != req.StartVersion {
			// 告诉客户端重试
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "true",
				},
			}, nil
		}
		// 2. 如果查找到锁并且锁是自己加的， 写入write 并释放锁
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.CommitResponse{
				RegionError: regionErr.RequestErr,
			}, nil
		}
		return nil, err
	}
	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if rangeErr, ok := err.(*raft_storage.RegionError); ok {
			return &kvrpcpb.ScanResponse{
				RegionError: rangeErr.RequestErr,
			}, nil
		}
		return nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)

	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var kv = make([]*kvrpcpb.KvPair, int(req.Limit))
	for i := 0; i < int(req.Limit); i++ {
		k, v, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		// 查看当前k上有没有所
		lock, err := txn.GetLock(k)
		if err != nil {
			if rangeErr, ok := err.(*raft_storage.RegionError); ok {
				return &kvrpcpb.ScanResponse{
					RegionError: rangeErr.RequestErr,
				}, nil
			}
			return nil, err
		}
		// 如果存在锁,或者锁是在读之前加的
		if lock != nil || req.Version > lock.Ts {
			kv[i] = &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         k,
						LockTtl:     lock.Ttl,
					},
				},
				Key:   k,
				Value: v,
			}
			return &kvrpcpb.ScanResponse{
				Pairs: kv,
			}, nil
		}

		kv[i] = &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		}

	}

	return &kvrpcpb.ScanResponse{
		Pairs: kv,
	}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

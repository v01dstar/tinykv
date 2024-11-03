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
	resp := new(kvrpcpb.RawGetResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if len(value) == 0 {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := new(kvrpcpb.RawPutResponse)
	batch := []storage.Modify{{
		Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf},
	}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := new(kvrpcpb.RawDeleteResponse)
	batch := []storage.Modify{{
		Data: storage.Delete{Key: req.Key, Cf: req.Cf},
	}}
	err := server.storage.Write(req.Context, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := new(kvrpcpb.RawScanResponse)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	var pairs []*kvrpcpb.KvPair
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			resp.Error = err.Error()
			return resp, err
		}
		pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
		if len(pairs) == int(req.Limit) {
			break
		}
	}
	resp.Kvs = pairs
	return resp, nil
}

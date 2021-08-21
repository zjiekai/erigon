package state

import (
	"bytes"
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

var _ StateReader = (*NfPlainStateReader)(nil)

// NfPlainStateReader reads data from so called "plain state".
// Data in the plain state is stored using un-hashed account/storage items
// as opposed to the "normal" state that uses hashes of merkle paths to store items.
type NfPlainStateReader struct {
	db kv.Getter
}

func NewNfPlainStateReader(db kv.Getter) *NfPlainStateReader {
	return &NfPlainStateReader{
		db: db,
	}
}

func (r *NfPlainStateReader) ReadAccountData(address common.Address) (*accounts.Account, error) {
	encID, err := r.db.GetOne(kv.AccountID, address.Bytes())
	if err != nil {
		return nil, err
	}
	enc, err := r.db.GetOne(kv.PlainState, encID)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err = a.DecodeForStorage(enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *NfPlainStateReader) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	encID, err := r.db.GetOne(kv.AccountID, address.Bytes())
	if err != nil {
		return nil, err
	}
	compositeKey := make([]byte, 8+common.IncarnationLength+common.HashLength)
	copy(compositeKey, encID)
	binary.BigEndian.PutUint64(compositeKey[common.AddressLength:], incarnation)
	copy(compositeKey[common.AddressLength+common.IncarnationLength:], key[:])
	enc, err := r.db.GetOne(kv.PlainState, compositeKey)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	return enc, nil
}

func (r *NfPlainStateReader) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if bytes.Equal(codeHash.Bytes(), emptyCodeHash) {
		return nil, nil
	}
	code, err := r.db.GetOne(kv.Code, codeHash.Bytes())
	if len(code) == 0 {
		return nil, nil
	}
	return code, err
}

func (r *NfPlainStateReader) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	code, err := r.ReadAccountCode(address, incarnation, codeHash)
	return len(code), err
}

func (r *NfPlainStateReader) ReadAccountIncarnation(address common.Address) (uint64, error) {
	b, err := r.db.GetOne(kv.IncarnationMap, address.Bytes())
	if err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(b), nil
}

package state

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

var _ WriterWithChangeSets = (*PlainStateWriter)(nil)

type NfPlainStateWriter struct {
	db          putDelSeq
	csw         *ChangeSetWriter
	accumulator *shards.Accumulator
}

type putDelSeq interface {
	putDel
	kv.Getter
	IncrementSequence(bucket string, amount uint64) (uint64, error)
}

func NewNfPlainStateWriter(db putDelSeq, changeSetsDB kv.RwTx, blockNumber uint64) *NfPlainStateWriter {
	return &NfPlainStateWriter{
		db:  db,
		csw: NewChangeSetWriterPlain(changeSetsDB, blockNumber),
	}
}

func NewNfPlainStateWriterNoHistory(db putDel) *PlainStateWriter {
	return &PlainStateWriter{
		db: db,
	}
}

func (w *NfPlainStateWriter) SetAccumulator(accumulator *shards.Accumulator) *NfPlainStateWriter {
	w.accumulator = accumulator
	return w
}

func (w *NfPlainStateWriter) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	//fmt.Printf("update: %x,%d,%d\n", address, account.Incarnation, account.Balance.Uint64())
	if w.csw != nil {
		if err := w.csw.UpdateAccountData(address, original, account); err != nil {
			return err
		}
	}
	value := make([]byte, account.EncodingLengthForStorage())
	account.EncodeForStorage(value)
	if w.accumulator != nil {
		w.accumulator.ChangeAccount(address, account.Incarnation, value)
	}

	encID, err := w.db.GetOne(kv.AccountID, address[:])
	if err != nil {
		return err
	}
	if encID == nil {
		id, err := w.db.IncrementSequence(kv.AccountID, 1)
		if err != nil {
			return err
		}
		encID = make([]byte, 8)
		binary.BigEndian.PutUint64(encID, id)
		err = w.db.Put(kv.AccountID, address[:], encID)
		if err != nil {
			return err
		}
	}
	//fmt.Printf("update2: %x\n", encID)

	return w.db.Put(kv.PlainState, encID, value)
}

func (w *NfPlainStateWriter) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if w.csw != nil {
		if err := w.csw.UpdateAccountCode(address, incarnation, codeHash, code); err != nil {
			return err
		}
	}
	if w.accumulator != nil {
		w.accumulator.ChangeCode(address, incarnation, code)
	}
	if err := w.db.Put(kv.Code, codeHash[:], code); err != nil {
		return err
	}
	return w.db.Put(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], incarnation), codeHash[:])
}

func (w *NfPlainStateWriter) DeleteAccount(address common.Address, original *accounts.Account) error {
	//fmt.Printf("del: %x\n", address)
	if w.csw != nil {
		if err := w.csw.DeleteAccount(address, original); err != nil {
			return err
		}
	}
	if w.accumulator != nil {
		w.accumulator.DeleteAccount(address)
	}
	encID, err := w.db.GetOne(kv.AccountID, address[:])
	if err != nil {
		return err
	}
	if encID == nil {
		id, err := w.db.IncrementSequence(kv.AccountID, 1)
		if err != nil {
			return err
		}
		encID = make([]byte, 8)
		binary.BigEndian.PutUint64(encID, id)
		err = w.db.Put(kv.AccountID, address[:], encID)
		if err != nil {
			return err
		}
	}
	if err := w.db.Delete(kv.PlainState, encID, nil); err != nil {
		return err
	}
	//if err := w.db.Delete(kv.AccountID, address[:], nil); err != nil {
	//	return err
	//}
	if original.Incarnation > 0 {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], original.Incarnation)
		if err := w.db.Put(kv.IncarnationMap, address[:], b[:]); err != nil {
			return err
		}
	}
	return nil
}

func (w *NfPlainStateWriter) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if w.csw != nil {
		if err := w.csw.WriteAccountStorage(address, incarnation, key, original, value); err != nil {
			return err
		}
	}
	if *original == *value {
		return nil
	}
	encID, err := w.db.GetOne(kv.AccountID, address[:])
	if err != nil {
		return err
	}
	if encID == nil {
		id, err := w.db.IncrementSequence(kv.AccountID, 1)
		if err != nil {
			return err
		}
		encID = make([]byte, 8)
		binary.BigEndian.PutUint64(encID, id)
		err = w.db.Put(kv.AccountID, address[:], encID)
		if err != nil {
			return err
		}
	}

	compositeKey := make([]byte, 8+common.IncarnationLength+common.HashLength)
	copy(compositeKey, encID)
	binary.BigEndian.PutUint64(compositeKey[8:], incarnation)
	copy(compositeKey[8+common.IncarnationLength:], key[:])

	v := value.Bytes()
	if w.accumulator != nil {
		w.accumulator.ChangeStorage(address, incarnation, *key, v)
	}
	if len(v) == 0 {
		return w.db.Delete(kv.PlainState, compositeKey, nil)
	}
	return w.db.Put(kv.PlainState, compositeKey, v)
}

func (w *NfPlainStateWriter) CreateContract(address common.Address) error {
	if w.csw != nil {
		if err := w.csw.CreateContract(address); err != nil {
			return err
		}
	}
	return nil
}

func (w *NfPlainStateWriter) WriteChangeSets() error {
	if w.csw != nil {
		return w.csw.WriteChangeSets()
	}

	return nil
}

func (w *NfPlainStateWriter) WriteHistory() error {
	if w.csw != nil {
		return w.csw.WriteHistory()
	}

	return nil
}

func (w *NfPlainStateWriter) ChangeSetWriter() *ChangeSetWriter {
	return w.csw
}

package postgres

import (
	"database/sql"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/algorand/go-algorand/protocol"
)

// Implements `ledgerForEvaluator` interface from go-algorand and is used for accounting.
type ledgerForEvaluator struct {
	tx *sql.Tx
	genesisHash crypto.Digest
}

func (l *ledgerForEvaluator) BlockHdr(round basics.Round) (bookkeeping.BlockHeader, error) {
	query := "SELECT header FROM block_header WHERE round = $1"
	row := l.tx.QueryRow(query, uint64(round))

	var blockheaderjson []byte
	err := row.Scan(&blockheaderjson)
	if err != nil {
		return bookkeeping.BlockHeader{}, err
	}

	var blockHeader bookkeeping.BlockHeader
	err = protocol.DecodeJSON(blockheaderjson, &blockHeader)

	return blockHeader, err
}

func (l *ledgerForEvaluator) CheckDup(config.ConsensusParams, basics.Round, basics.Round, basics.Round, transactions.Txid, ledger.TxLease) error {
	// This function is not used by evaluator.
	return nil
}

func (l *ledgerForEvaluator) LookupWithoutRewards(round basics.Round, address basics.Address) (basics.AccountData, basics.Round, error) {
	// TODO
	return basics.AccountData{}, 0, nil
}

func (l *ledgerForEvaluator) GetCreatorForRound(round basics.Round, cindex basics.CreatableIndex, ctype basics.CreatableType) (basics.Address, bool, error) {
	var query string
	switch ctype {
	case basics.AssetCreatable:
		query = "SELECT creator_addr FROM asset WHERE index = $1"
	case basics.AppCreatable:
		query = "SELECT creator FROM app WHERE index = $1"
	default:
		panic("unknown creatable type")
	}

	row := l.tx.QueryRow(query, uint64(cindex))

	var buf []byte
	err := row.Scan(&buf)
	if err == sql.ErrNoRows {
		return basics.Address{}, false, nil
	}
	if err != nil {
		return basics.Address{}, false, nil
	}

	var address basics.Address
	copy(address[:], buf)

	return address, true, nil
}

func (l *ledgerForEvaluator) GenesisHash() crypto.Digest {
	return l.genesisHash
}

func (l *ledgerForEvaluator) Totals(round basics.Round) (ledgercore.AccountTotals, error) {
	// TODO
	return ledgercore.AccountTotals{}, nil
}

func (l *ledgerForEvaluator) CompactCertVoters(basics.Round) (*ledger.VotersForRound, error) {
	// This function is not used by evaluator.
	return nil, nil
}

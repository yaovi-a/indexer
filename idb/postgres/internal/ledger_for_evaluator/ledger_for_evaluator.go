package ledgerforevaluator

import (
	"context"
	"errors"
	"fmt"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"

	"github.com/algorand/indexer/idb/postgres/internal/encoding"
)

const (
	blockHeaderStmtName = "block_header"
	assetCreatorStmtName = "asset_creator"
	appCreatorStmtName = "app_creator"
	accountStmtName = "account"
	assetHoldingsStmtName = "asset_holdings"
	assetParamsStmtName = "asset_params"
	appParamsStmtName = "app_params"
	appLocalStatesStmtName = "app_local_states"
)

var statements = map[string]string{
	blockHeaderStmtName: "SELECT header FROM block_header WHERE round = $1",
	assetCreatorStmtName: "SELECT creator_addr FROM asset " +
	"WHERE index = $1 AND NOT deleted",
	appCreatorStmtName: "SELECT creator FROM app WHERE index = $1 AND NOT deleted",
	accountStmtName: "SELECT microalgos, rewardsbase, rewards_total, account_data " +
	"FROM account WHERE addr = $1 AND NOT deleted",
	assetHoldingsStmtName: "SELECT assetid, amount, frozen FROM account_asset " +
	"WHERE addr = $1 AND NOT deleted",
	assetParamsStmtName: "SELECT index, params FROM asset " +
	"WHERE creator_addr = $1 AND NOT deleted",
	appParamsStmtName: "SELECT index, params FROM app WHERE creator = $1 AND NOT deleted",
	appLocalStatesStmtName: "SELECT app, localstate FROM account_app " +
	"WHERE addr = $1 AND NOT deleted",
}

// LedgerForEvaluator implements the ledgerForEvaluator interface from
// go-algorand ledger/eval.go and is used for accounting.
type LedgerForEvaluator struct {
	tx          pgx.Tx
	genesisHash crypto.Digest
	// Indexer currently does not store the balances of special account, but
	// go-algorand's eval checks that they satisfy the minimum balance. We thus return
	// a fake amount.
	// TODO: remove.
	specialAddresses transactions.SpecialAddresses
	log      *log.Logger
	// Value is nil if account was looked up but not found.
	preloadedAccountData map[basics.Address]*basics.AccountData
}

// MakeLedgerForEvaluator creates a LedgerForEvaluator object.
func MakeLedgerForEvaluator(tx pgx.Tx, genesisHash crypto.Digest, specialAddresses transactions.SpecialAddresses, log *log.Logger) (LedgerForEvaluator, error) {
	l := LedgerForEvaluator{
		tx:               tx,
		genesisHash:      genesisHash,
		specialAddresses: specialAddresses,
		log: log,
	}

	for name, query := range statements {
		_, err := tx.Prepare(context.Background(), name, query)
		if err != nil {
			return LedgerForEvaluator{},
				fmt.Errorf("MakeLedgerForEvaluator() prepare statement err: %w", err)
		}
	}

	return l, nil
}

// Close shuts down LedgerForEvaluator.
func (l *LedgerForEvaluator) Close() {
	for name := range statements {
		l.tx.Conn().Deallocate(context.Background(), name)
	}
}

// BlockHdr is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) BlockHdr(round basics.Round) (bookkeeping.BlockHeader, error) {
	row := l.tx.QueryRow(context.Background(), blockHeaderStmtName, uint64(round))

	var header []byte
	err := row.Scan(&header)
	if err != nil {
		return bookkeeping.BlockHeader{}, fmt.Errorf("BlockHdr() scan row err: %w", err)
	}

	res, err := encoding.DecodeBlockHeader(header)
	if err != nil {
		return bookkeeping.BlockHeader{}, fmt.Errorf("BlockHdr() decode header err: %w", err)
	}

	return res, nil
}

// CheckDup is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) CheckDup(config.ConsensusParams, basics.Round, basics.Round, basics.Round, transactions.Txid, ledger.TxLease) error {
	// This function is not used by evaluator.
	return errors.New("CheckDup() not implemented")
}

func (l *LedgerForEvaluator) isSpecialAddress(address basics.Address) bool {
	return (address == l.specialAddresses.FeeSink) ||
		(address == l.specialAddresses.RewardsPool)
}

func (l *LedgerForEvaluator) parseAccountTable(address basics.Address, row pgx.Row) (basics.AccountData, bool /*exists*/, error) {
	var microalgos uint64
	var rewardsbase uint64
	var rewardsTotal uint64
	var accountData []byte

	err := row.Scan(&microalgos, &rewardsbase, &rewardsTotal, &accountData)
	if err == pgx.ErrNoRows {
		return basics.AccountData{}, false, nil
	}
	if err != nil {
		return basics.AccountData{}, false, fmt.Errorf("readAccountTable() scan row err: %w", err)
	}

	res := basics.AccountData{}
	if accountData != nil {
		res, err = encoding.DecodeTrimmedAccountData(accountData)
		if err != nil {
			return basics.AccountData{}, false,
				fmt.Errorf("readAccountTable() decode account data err: %w", err)
		}
	}

	res.MicroAlgos = basics.MicroAlgos{Raw: microalgos}
	res.RewardsBase = rewardsbase
	res.RewardedMicroAlgos = basics.MicroAlgos{Raw: rewardsTotal}

	return res, true, nil
}

func (l *LedgerForEvaluator) readAccountTable(address basics.Address) (basics.AccountData, bool /*exists*/, error) {
	row := l.tx.QueryRow(context.Background(), accountStmtName, address[:])
	return l.parseAccountTable(address, row)
}

func (l *LedgerForEvaluator) parseAccountAssetTable(address basics.Address, rows pgx.Rows) (map[basics.AssetIndex]basics.AssetHolding, error) {
	res := make(map[basics.AssetIndex]basics.AssetHolding)

	var assetid uint64
	var amount uint64
	var frozen bool

	for rows.Next() {
		err := rows.Scan(&assetid, &amount, &frozen)
		if err != nil {
			return nil, fmt.Errorf("readAccountAssetTable() scan row err: %w", err)
		}

		res[basics.AssetIndex(assetid)] = basics.AssetHolding{
			Amount: amount,
			Frozen: frozen,
		}
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("readAccountAssetTable() scan end err: %w", err)
	}

	return res, nil
}

func (l *LedgerForEvaluator) readAccountAssetTable(address basics.Address) (map[basics.AssetIndex]basics.AssetHolding, error) {
	rows, err := l.tx.Query(context.Background(), assetHoldingsStmtName, address[:])
	if err != nil {
		return nil, fmt.Errorf("readAccountAssetTable() query err: %w", err)
	}

	return l.parseAccountAssetTable(address, rows)
}

func (l *LedgerForEvaluator) parseAssetTable(address basics.Address, rows pgx.Rows) (map[basics.AssetIndex]basics.AssetParams, error) {
	res := make(map[basics.AssetIndex]basics.AssetParams)

	var index uint64
	var params []byte

	for rows.Next() {
		err := rows.Scan(&index, &params)
		if err != nil {
			return nil, fmt.Errorf("readAssetTable() scan row err: %w", err)
		}

		res[basics.AssetIndex(index)], err = encoding.DecodeAssetParams(params)
		if err != nil {
			return nil, fmt.Errorf("readAssetTable() decode params err: %w", err)
		}
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("readAssetTable() scan end err: %w", err)
	}

	return res, nil
}

func (l *LedgerForEvaluator) readAssetTable(address basics.Address) (map[basics.AssetIndex]basics.AssetParams, error) {
	rows, err := l.tx.Query(context.Background(), assetParamsStmtName, address[:])
	if err != nil {
		return nil, fmt.Errorf("readAssetTable() query err: %w", err)
	}

	return l.parseAssetTable(address, rows)
}

func (l *LedgerForEvaluator) parseAppTable(address basics.Address, rows pgx.Rows) (map[basics.AppIndex]basics.AppParams, error) {
	res := make(map[basics.AppIndex]basics.AppParams)

	var index uint64
	var params []byte

	for rows.Next() {
		err := rows.Scan(&index, &params)
		if err != nil {
			return nil, fmt.Errorf("readAppTable() scan row err: %w", err)
		}

		res[basics.AppIndex(index)], err = encoding.DecodeAppParams(params)
		if err != nil {
			return nil, fmt.Errorf("readAppTable() decode params err: %w", err)
		}
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("readAppTable() scan end err: %w", err)
	}

	return res, nil
}

func (l *LedgerForEvaluator) readAppTable(address basics.Address) (map[basics.AppIndex]basics.AppParams, error) {
	rows, err := l.tx.Query(context.Background(), appParamsStmtName, address[:])
	if err != nil {
		return nil, fmt.Errorf("readAppTable() query err: %w", err)
	}

	return l.parseAppTable(address, rows)
}

func (l *LedgerForEvaluator) parseAccountAppTable(address basics.Address, rows pgx.Rows) (map[basics.AppIndex]basics.AppLocalState, error) {
	res := make(map[basics.AppIndex]basics.AppLocalState)

	var app uint64
	var localstate []byte

	for rows.Next() {
		err := rows.Scan(&app, &localstate)
		if err != nil {
			return nil, fmt.Errorf("readAccountAppTable() scan row err: %w", err)
		}

		res[basics.AppIndex(app)], err = encoding.DecodeAppLocalState(localstate)
		if err != nil {
			return nil, fmt.Errorf("readAccountAppTable() decode local state err: %w", err)
		}
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("readAccountAppTable() scan end err: %w", err)
	}

	return res, nil
}

func (l *LedgerForEvaluator) readAccountAppTable(address basics.Address) (map[basics.AppIndex]basics.AppLocalState, error) {
	rows, err := l.tx.Query(context.Background(), appLocalStatesStmtName, address[:])
	if err != nil {
		return nil, fmt.Errorf("readAccountAppTable() query err: %w", err)
	}

	return l.parseAccountAppTable(address, rows)
}

func (l *LedgerForEvaluator) loadAccountTable(addresses []basics.Address) error {
	l.preloadedAccountData = make(map[basics.Address]*basics.AccountData, len(addresses))

	var batch pgx.Batch
	for i := range addresses {
		if !l.isSpecialAddress(addresses[i]) {
			batch.Queue(accountStmtName, addresses[i][:])
		}
	}

	results := l.tx.SendBatch(context.Background(), &batch)
	for _, address := range addresses {
		if !l.isSpecialAddress(address) {
			row := results.QueryRow()

			accountData := new(basics.AccountData)
			var exists bool
			var err error

			*accountData, exists, err = l.parseAccountTable(address, row)
			if err != nil {
				return fmt.Errorf("loadAccountTable() err: %w", err)
			}

			if exists {
				l.preloadedAccountData[address] = accountData
			} else {
				l.preloadedAccountData[address] = nil
			}
		}
	}

	err := results.Close()
	if err != nil {
		return fmt.Errorf("loadAccountTable() close results err: %w", err)
	}

	return nil
}

func (l *LedgerForEvaluator) loadCreatables(addresses []basics.Address) error {
	var batch pgx.Batch

	existingAddresses := make([]basics.Address, 0, len(addresses))
	for _, address := range addresses {
		if l.preloadedAccountData[address] != nil {
			existingAddresses = append(existingAddresses, address)
		}
	}

	for i := range existingAddresses {
		batch.Queue(assetHoldingsStmtName, existingAddresses[i][:])
	}
	for i := range existingAddresses {
		batch.Queue(assetParamsStmtName, existingAddresses[i][:])
	}
	for i := range existingAddresses {
		batch.Queue(appParamsStmtName, existingAddresses[i][:])
	}
	for i := range existingAddresses {
		batch.Queue(appLocalStatesStmtName, existingAddresses[i][:])
	}

	results := l.tx.SendBatch(context.Background(), &batch)

	for _, address := range existingAddresses {
		rows, err := results.Query()
		if err != nil {
			return fmt.Errorf("loadCreatables() query asset holdings err: %w", err)
		}
		l.preloadedAccountData[address].Assets, err = l.parseAccountAssetTable(address, rows)
		if err != nil {
			return fmt.Errorf("loadCreatables() err: %w", err)
		}
	}
	for _, address := range existingAddresses {
		rows, err := results.Query()
		if err != nil {
			return fmt.Errorf("loadCreatables() query asset params err: %w", err)
		}
		l.preloadedAccountData[address].AssetParams, err = l.parseAssetTable(address, rows)
		if err != nil {
			return fmt.Errorf("loadCreatables() err: %w", err)
		}
	}
	for _, address := range existingAddresses {
		rows, err := results.Query()
		if err != nil {
			return fmt.Errorf("loadCreatables() query app params err: %w", err)
		}
		l.preloadedAccountData[address].AppParams, err = l.parseAppTable(address, rows)
		if err != nil {
			return fmt.Errorf("loadCreatables() err: %w", err)
		}
	}
	for _, address := range existingAddresses {
		rows, err := results.Query()
		if err != nil {
			return fmt.Errorf("loadCreatables() query app local states err: %w", err)
		}
		l.preloadedAccountData[address].AppLocalStates, err =
			l.parseAccountAppTable(address, rows)
		if err != nil {
			return fmt.Errorf("loadCreatables() err: %w", err)
		}
	}

	err := results.Close()
	if err != nil {
		return fmt.Errorf("loadCreatables() close results err: %w", err)
	}

	return nil
}

func (l *LedgerForEvaluator) PreloadAccounts(addresses []basics.Address) error {
	err := l.loadAccountTable(addresses)
	if err != nil {
		return fmt.Errorf("PreloadAccounts() err: %w", err)
	}

	err = l.loadCreatables(addresses)
	if err != nil {
		return fmt.Errorf("PreloadAccounts() err: %w", err)
	}

	return nil
}

func (l *LedgerForEvaluator) fetchAccountData(address basics.Address) (basics.AccountData, error) {
	accountData, exists, err := l.readAccountTable(address)
	if err != nil {
		return basics.AccountData{}, err
	}
	if !exists {
		return basics.AccountData{}, nil
	}

	accountData.Assets, err = l.readAccountAssetTable(address)
	if err != nil {
		return basics.AccountData{}, err
	}

	accountData.AssetParams, err = l.readAssetTable(address)
	if err != nil {
		return basics.AccountData{}, err
	}

	accountData.AppParams, err = l.readAppTable(address)
	if err != nil {
		return basics.AccountData{}, err
	}

	accountData.AppLocalStates, err = l.readAccountAppTable(address)
	if err != nil {
		return basics.AccountData{}, err
	}

	return accountData, nil
}

// LookupWithoutRewards is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) LookupWithoutRewards(round basics.Round, address basics.Address) (basics.AccountData, basics.Round, error) {
	// The balance of a special address must pass the minimum balance check in
	// go-algorand's evaluator, so return a sufficiently large balance.
	if l.isSpecialAddress(address) {
		var balance uint64 = 1000 * 1000 * 1000 * 1000 * 1000
		accountData := basics.AccountData{
			MicroAlgos: basics.MicroAlgos{Raw: balance},
		}
		return accountData, round, nil
	}

	if accountData, ok := l.preloadedAccountData[address]; ok {
		if accountData != nil {
			return *accountData, round, nil
		} else {
			return basics.AccountData{}, round, nil
		}
	}

	if l.log != nil {
		l.log.Warnf("account %s was not preloaded", address)
	}

	accountData, err := l.fetchAccountData(address)
	if err != nil {
		return basics.AccountData{}, basics.Round(0), err
	}
	return accountData, round, nil
}

// GetCreatorForRound is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) GetCreatorForRound(_ basics.Round, cindex basics.CreatableIndex, ctype basics.CreatableType) (basics.Address, bool, error) {
	var row pgx.Row

	switch ctype {
	case basics.AssetCreatable:
		row = l.tx.QueryRow(context.Background(), assetCreatorStmtName, uint64(cindex))
	case basics.AppCreatable:
		row = l.tx.QueryRow(context.Background(), appCreatorStmtName, uint64(cindex))
	default:
		panic("unknown creatable type")
	}

	var buf []byte
	err := row.Scan(&buf)
	if err == pgx.ErrNoRows {
		return basics.Address{}, false, nil
	}
	if err != nil {
		return basics.Address{}, false, nil
	}

	var address basics.Address
	copy(address[:], buf)

	return address, true, nil
}

// GenesisHash is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) GenesisHash() crypto.Digest {
	return l.genesisHash
}

// Totals is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) Totals(round basics.Round) (ledgercore.AccountTotals, error) {
	// The evaluator uses totals only for recomputing the rewards pool balance. Indexer
	// does not currently compute this balance, and we can return an empty struct
	// here.
	return ledgercore.AccountTotals{}, nil
}

// CompactCertVoters is part of go-algorand's ledgerForEvaluator interface.
func (l LedgerForEvaluator) CompactCertVoters(basics.Round) (*ledger.VotersForRound, error) {
	// This function is not used by evaluator.
	return nil, errors.New("CompactCertVoters() not implemented")
}

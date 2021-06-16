package postgres

import (
	//"context"
	"context"
	"database/sql"

	//"fmt"
	"sync"
	"testing"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/protocol"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres/internal/encoding"
	"github.com/algorand/indexer/util/test"
)

// TestMaxRoundOnUninitializedDB makes sure we return 0 when getting the max round on a new DB.
func TestMaxRoundOnUninitializedDB(t *testing.T) {
	_, connStr, shutdownFunc := setupPostgres(t)
	defer shutdownFunc()

	///////////
	// Given // A database that has not yet imported the genesis accounts.
	///////////
	db, err := OpenPostgres(connStr, idb.IndexerDbOptions{}, nil)
	assert.NoError(t, err)

	//////////
	// When // We request the max round.
	//////////
	roundA, errA := db.GetMaxRoundAccounted()
	roundL, errL := db.getNextRoundToLoad()

	//////////
	// Then // The error message should be set.
	//////////
	assert.Equal(t, errA, idb.ErrorNotInitialized)
	assert.Equal(t, uint64(0), roundA)

	require.NoError(t, errL)
	assert.Equal(t, uint64(0), roundL)
}

// TestMaxRoundEmptyMetastate makes sure we return 0 when the metastate is empty.
func TestMaxRoundEmptyMetastate(t *testing.T) {
	pg, connStr, shutdownFunc := setupPostgres(t)
	defer shutdownFunc()
	///////////
	// Given // The database has the metastate set but the account_round is missing.
	///////////
	db, err := idb.IndexerDbByName("postgres", connStr, idb.IndexerDbOptions{}, nil)
	assert.NoError(t, err)
	pg.Exec(`INSERT INTO metastate (k, v) values ('state', '{}')`)

	//////////
	// When // We request the max round.
	//////////
	round, err := db.GetMaxRoundAccounted()

	//////////
	// Then // The error message should be set.
	//////////
	assert.Equal(t, err, idb.ErrorNotInitialized)
	assert.Equal(t, uint64(0), round)
}

// TestMaxRound the happy path.
func TestMaxRound(t *testing.T) {
	db, connStr, shutdownFunc := setupPostgres(t)
	defer shutdownFunc()
	///////////
	// Given // The database has the metastate set normally.
	///////////
	pdb, err := OpenPostgres(connStr, idb.IndexerDbOptions{}, nil)
	assert.NoError(t, err)
	db.Exec(`INSERT INTO metastate (k, v) values ($1, $2)`, "state", "{\"account_round\":123454321}")
	db.Exec(`INSERT INTO block_header (round, realtime, rewardslevel, header) VALUES ($1, NOW(), 0, '{}') ON CONFLICT DO NOTHING`, 543212345)

	//////////
	// When // We request the max round.
	//////////
	roundA, err := pdb.GetMaxRoundAccounted()
	assert.NoError(t, err)
	roundL, err := pdb.getNextRoundToLoad()
	assert.NoError(t, err)

	//////////
	// Then // There should be no error and we return that there are zero rounds.
	//////////
	assert.Equal(t, uint64(123454321), roundA)
	assert.Equal(t, uint64(543212346), roundL)
}

func assertAccountAsset(t *testing.T, db *sql.DB, addr basics.Address, assetid uint64, frozen bool, amount uint64) {
	var row *sql.Row
	var f bool
	var a uint64

	row = db.QueryRow(`SELECT frozen, amount FROM account_asset as a WHERE a.addr = $1 AND assetid = $2`, addr[:], assetid)
	err := row.Scan(&f, &a)
	assert.NoError(t, err, "failed looking up AccountA.")
	assert.Equal(t, frozen, f)
	assert.Equal(t, amount, a)
}

// TestAssetCloseReopenTransfer tests a scenario that requires asset subround accounting
func TestAssetCloseReopenTransfer(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetid := uint64(1)
	amt := uint64(10000)
	total := uint64(1000000)

	///////////
	// Given // A round scenario requiring subround accounting: AccountA is funded, closed, opts back, and funded again.
	///////////
	createAsset := test.MakeCreateAssetTxn(
		total, uint64(6), false, "mcn", "my coin", "http://antarctica.com", test.AccountD)
	optInA := test.MakeAssetOptInTxn(assetid, test.AccountA)
	fundA := test.MakeAssetTransferTxn(
		assetid, amt, test.AccountD, test.AccountA, basics.Address{})
	optInB := test.MakeAssetOptInTxn(assetid, test.AccountB)
	optInC := test.MakeAssetOptInTxn(assetid, test.AccountC)
	closeA := test.MakeAssetTransferTxn(
		assetid, 1000, test.AccountA, test.AccountB, test.AccountC)
	payMain := test.MakeAssetTransferTxn(
		assetid, amt, test.AccountD, test.AccountA, basics.Address{})

	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &createAsset, &optInA, &fundA, &optInB,
		&optInC, &closeA, &optInA, &payMain)
	require.NoError(t, err)

	//////////
	// When // We commit the block to the database
	//////////
	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Accounts A, B, C and D have the correct balances.
	//////////
	// A has the final payment after being closed out
	assertAccountAsset(t, db.db, test.AccountA, assetid, false, amt)
	// B has the closing transfer amount
	assertAccountAsset(t, db.db, test.AccountB, assetid, false, 1000)
	// C has the close-to remainder
	assertAccountAsset(t, db.db, test.AccountC, assetid, false, 9000)
	// D has the total minus both payments to A
	assertAccountAsset(t, db.db, test.AccountD, assetid, false, total-2*amt)
}

// TestReCreateAssetHolding checks the optin value of a defunct
func TestReCreateAssetHolding(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	total := uint64(1000000)

	block := test.MakeGenesisBlock()
	for i, frozen := range []bool{true, false} {
		assetid := uint64(1 + 5*i)
		///////////
		// Given //
		// A new asset with default-frozen, AccountB opts-in and has its frozen state
		// toggled.
		/////////// Then AccountB opts-out then opts-in again.
		createAssetFrozen := test.MakeCreateAssetTxn(
			total, uint64(6), frozen, "icicles", "frozen coin",
			"http://antarctica.com", test.AccountA)
		optinB := test.MakeAssetOptInTxn(assetid, test.AccountB)
		unfreezeB := test.MakeAssetFreezeTxn(
			assetid, !frozen, test.AccountA, test.AccountB)
		optoutB := test.MakeAssetTransferTxn(
			assetid, 0, test.AccountB, test.AccountC, test.AccountD)

		var err error
		block, err = test.MakeBlockForTxns(
			block.BlockHeader, &createAssetFrozen, &optinB, &unfreezeB,
			&optoutB, &optinB)
		require.NoError(t, err)

		//////////
		// When // We commit the round accounting to the database.
		//////////
		err = db.AddBlock(block)
		require.NoError(t, err)

		//////////
		// Then // AccountB should have its frozen state set back to the default value
		//////////
		assertAccountAsset(t, db.db, test.AccountB, assetid, frozen, 0)
	}
}

// TestMultipleAssetOptins make sure no-op transactions don't reset the default frozen value.
func TestNoopOptins(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	///////////
	// Given //
	// An asset with default-frozen = true, AccountB opts in, is unfrozen, then has a
	// no-op opt-in
	///////////
	assetid := uint64(1)

	createAsset := test.MakeCreateAssetTxn(
		uint64(1000000), uint64(6), true, "icicles", "frozen coin",
		"http://antarctica.com", test.AccountD)
	optinB := test.MakeAssetOptInTxn(assetid, test.AccountB)
	unfreezeB := test.MakeAssetFreezeTxn(assetid, false, test.AccountD, test.AccountB)

	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &createAsset, &optinB, &unfreezeB, &optinB)
	require.NoError(t, err)

	//////////
	// When // We commit the round accounting to the database.
	//////////
	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // AccountB should have its frozen state set back to the default value
	//////////
	assertAccountAsset(t, db.db, test.AccountB, assetid, false, 0)
}

// TestMultipleWriters tests that accounting cannot be double committed.
func TestMultipleWriters(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	amt := uint64(10000)

	///////////
	// Given // Send amt to AccountE
	///////////
	payAccountE := test.MakePaymentTxn(
		1000, amt, 0, 0, 0, 0, test.AccountD, test.AccountE, basics.Address{},
		basics.Address{})

	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &payAccountE)
	require.NoError(t, err)

	//////////
	// When // We attempt commit the round accounting multiple times.
	//////////
	start := make(chan struct{})
	commits := 10
	errors := make(chan error, commits)
	var wg sync.WaitGroup
	for i := 0; i < commits; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errors <- db.AddBlock(block)
		}()
	}
	close(start)

	wg.Wait()
	close(errors)

	//////////
	// Then // There should be num-1 errors, and AccountA should only be paid once.
	//////////
	errorCount := 0
	for err := range errors {
		if err != nil {
			errorCount++
		}
	}
	assert.Equal(t, commits-1, errorCount)

	// AccountE should contain the final payment.
	var balance uint64
	row := db.db.QueryRow(`SELECT microalgos FROM account WHERE account.addr = $1`, test.AccountE[:])
	err = row.Scan(&balance)
	assert.NoError(t, err, "checking balance")
	assert.Equal(t, amt, balance)
}

// TestBlockWithTransactions tests that the block with transactions endpoint works.
func TestBlockWithTransactions(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	round := uint64(1)
	assetid := uint64(1)
	amt := uint64(10000)
	total := uint64(1000000)

	///////////
	// Given // A block at round `round` with 5 transactions.
	///////////
	txn1 := test.MakeCreateAssetTxn(total, uint64(6), false, "icicles", "frozen coin", "http://antarctica.com", test.AccountD)
	txn2 := test.MakeAssetOptInTxn(assetid, test.AccountA)
	txn3 := test.MakeAssetTransferTxn(
		assetid, amt, test.AccountD, test.AccountA, basics.Address{})
	txn4 := test.MakeAssetOptInTxn(assetid, test.AccountB)
	txn5 := test.MakeAssetOptInTxn(assetid, test.AccountC)
	txn6 := test.MakeAssetTransferTxn(
		assetid, 1000, test.AccountA, test.AccountB, test.AccountC)
	txn7 := test.MakeAssetTransferTxn(
		assetid, 0, test.AccountA, test.AccountA, basics.Address{})
	txn8 := test.MakeAssetTransferTxn(
		assetid, amt, test.AccountD, test.AccountA, basics.Address{})
	txns := []*transactions.SignedTxnWithAD{
		&txn1, &txn2, &txn3, &txn4, &txn5, &txn6, &txn7, &txn8}

	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, txns...)
	require.NoError(t, err)
	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// When // We call GetBlock and Transactions
	//////////
	_, txnRows0, err := db.GetBlock(
		context.Background(), round, idb.GetBlockOptions{Transactions: true})
	require.NoError(t, err)

	rowsCh, _ := db.Transactions(context.Background(), idb.TransactionFilter{Round: &round})
	txnRows1 := make([]idb.TxnRow, 0)
	for row := range rowsCh {
		require.NoError(t, row.Error)
		txnRows1 = append(txnRows1, row)
	}

	//////////
	// Then // They should have the correct transactions
	//////////
	assert.Len(t, txnRows0, len(txns))
	assert.Len(t, txnRows1, len(txns))
	for i := 0; i < len(txnRows0); i++ {
		expected := protocol.Encode(txns[i])
		assert.Equal(t, expected, txnRows0[i].TxnBytes)
		assert.Equal(t, expected, txnRows1[i].TxnBytes)
	}
}

func TestRekeyBasic(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	///////////
	// Given // Send rekey transaction
	///////////
	txn := test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{}, test.AccountB)
	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &txn)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Account A is rekeyed to account B
	//////////
	var accountDataStr []byte
	row := db.db.QueryRow(`SELECT account_data FROM account WHERE account.addr = $1`, test.AccountA[:])
	err = row.Scan(&accountDataStr)
	assert.NoError(t, err, "querying account data")

	ad, err := encoding.DecodeAccountData(accountDataStr)
	require.NoError(t, err, "failed to parse account data json")
	assert.Equal(t, test.AccountB, ad.AuthAddr)
}

func TestRekeyToItself(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	///////////
	// Given // Send rekey transactions
	///////////
	txn := test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{}, test.AccountB)
	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &txn)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	txn = test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{},
		test.AccountA)
	block, err = test.MakeBlockForTxns(block.BlockHeader, &txn)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Account's A auth-address is not recorded
	//////////
	var accountDataStr []byte
	row := db.db.QueryRow(`SELECT account_data FROM account WHERE account.addr = $1`, test.AccountA[:])
	err = row.Scan(&accountDataStr)
	assert.NoError(t, err, "querying account data")

	ad, err := encoding.DecodeAccountData(accountDataStr)
	require.NoError(t, err, "failed to parse account data json")
	assert.Equal(t, basics.Address{}, ad.AuthAddr)
}

func TestRekeyThreeTimesInSameRound(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	///////////
	// Given // Send rekey transaction
	///////////
	txn0 := test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{},
		test.AccountB)
	txn1 := test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{},
		basics.Address{})
	txn2 := test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{}, test.AccountC)
	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &txn0, &txn1, &txn2)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Account A is rekeyed to account C
	//////////
	var accountDataStr []byte
	row := db.db.QueryRow(`SELECT account_data FROM account WHERE account.addr = $1`, test.AccountA[:])
	err = row.Scan(&accountDataStr)
	assert.NoError(t, err, "querying account data")

	ad, err := encoding.DecodeAccountData(accountDataStr)
	require.NoError(t, err, "failed to parse account data json")
	assert.Equal(t, test.AccountC, ad.AuthAddr)
}

func TestRekeyToItselfHasNotBeenRekeyed(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	///////////
	// Given // Send rekey transaction
	///////////
	txn := test.MakePaymentTxn(
		1000, 0, 0, 0, 0, 0, test.AccountA, test.AccountA, basics.Address{},
		basics.Address{})
	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &txn)
	require.NoError(t, err)

	//////////
	// Then // No error when committing to the DB.
	//////////
	err = db.AddBlock(block)
	require.NoError(t, err)
}

// TestIgnoreDefaultFrozenConfigUpdate the creator asset holding should ignore default-frozen = true.
func TestIgnoreDefaultFrozenConfigUpdate(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetid := uint64(1)
	total := uint64(1000000)

	///////////
	// Given // A new asset with default-frozen = true, and AccountB opting into it.
	///////////
	createAssetNotFrozen := test.MakeCreateAssetTxn(
		total, uint64(6), false, "icicles", "frozen coin", "http://antarctica.com",
		test.AccountA)
	modifyAssetToFrozen := test.MakeCreateAssetTxn(
		total, uint64(6), true, "icicles", "frozen coin", "http://antarctica.com",
		test.AccountA)
	optin := test.MakeAssetOptInTxn(assetid, test.AccountB)

	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &createAssetNotFrozen, &modifyAssetToFrozen,
		&optin)
	require.NoError(t, err)

	//////////
	// When // We commit the round accounting to the database.
	//////////
	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Make sure the accounts have the correct default-frozen after create/optin
	//////////
	// default-frozen = true
	assertAccountAsset(t, db.db, test.AccountA, assetid, false, total)
	assertAccountAsset(t, db.db, test.AccountB, assetid, false, 0)
}

// TestZeroTotalAssetCreate tests that the asset holding with total of 0 is created.
func TestZeroTotalAssetCreate(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetid := uint64(1)
	total := uint64(0)

	///////////
	// Given // A new asset with total = 0.
	///////////
	createAsset := test.MakeCreateAssetTxn(
		total, uint64(6), false, "mcn", "my coin", "http://antarctica.com",
		test.AccountA)
	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &createAsset)
	require.NoError(t, err)

	//////////
	// When // We commit the round accounting to the database.
	//////////
	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Make sure the creator has an asset holding with amount = 0.
	//////////
	assertAccountAsset(t, db.db, test.AccountA, assetid, false, 0)
}

func assertAssetDates(t *testing.T, db *sql.DB, assetID uint64, deleted sql.NullBool, createdAt sql.NullInt64, closedAt sql.NullInt64) {
	row := db.QueryRow(
		"SELECT deleted, created_at, closed_at FROM asset WHERE index = $1", int64(assetID))

	var retDeleted sql.NullBool
	var retCreatedAt sql.NullInt64
	var retClosedAt sql.NullInt64
	err := row.Scan(&retDeleted, &retCreatedAt, &retClosedAt)
	assert.NoError(t, err)

	assert.Equal(t, deleted, retDeleted)
	assert.Equal(t, createdAt, retCreatedAt)
	assert.Equal(t, closedAt, retClosedAt)
}

func assertAssetHoldingDates(t *testing.T, db *sql.DB, address basics.Address, assetID uint64, deleted sql.NullBool, createdAt sql.NullInt64, closedAt sql.NullInt64) {
	row := db.QueryRow(
		"SELECT deleted, created_at, closed_at FROM account_asset WHERE "+
			"addr = $1 AND assetid = $2",
		address[:], assetID)

	var retDeleted sql.NullBool
	var retCreatedAt sql.NullInt64
	var retClosedAt sql.NullInt64
	err := row.Scan(&retDeleted, &retCreatedAt, &retClosedAt)
	assert.NoError(t, err)

	assert.Equal(t, deleted, retDeleted)
	assert.Equal(t, createdAt, retCreatedAt)
	assert.Equal(t, closedAt, retClosedAt)
}

func TestDestroyAssetBasic(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetID := uint64(1)

	// Create an asset.
	txn := test.MakeCreateAssetTxn(4, 0, false, "uu", "aa", "", test.AccountA)
	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &txn)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	// Destroy an asset.
	txn = test.MakeAssetDestroyTxn(assetID, test.AccountA)
	block, err = test.MakeBlockForTxns(block.BlockHeader, &txn)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	// Check that the asset is deleted.
	assertAssetDates(t, db.db, assetID,
		sql.NullBool{Valid: true, Bool: true},
		sql.NullInt64{Valid: true, Int64: 1},
		sql.NullInt64{Valid: true, Int64: 2})

	// Check that the account's asset holding is deleted.
	assertAssetHoldingDates(t, db.db, test.AccountA, assetID,
		sql.NullBool{Valid: true, Bool: true},
		sql.NullInt64{Valid: true, Int64: 1},
		sql.NullInt64{Valid: true, Int64: 2})
}

func TestDestroyAssetZeroSupply(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetID := uint64(1)

	// Create an asset. Set total supply to 0.
	txn0 := test.MakeCreateAssetTxn(0, 0, false, "uu", "aa", "", test.AccountA)
	txn1 := test.MakeAssetDestroyTxn(assetID, test.AccountA)
	block, err := test.MakeBlockForTxns(test.MakeGenesisBlock().BlockHeader, &txn0, &txn1)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	// Check that the asset is deleted.
	assertAssetDates(t, db.db, assetID,
		sql.NullBool{Valid: true, Bool: true},
		sql.NullInt64{Valid: true, Int64: 1},
		sql.NullInt64{Valid: true, Int64: 1})

	// Check that the account's asset holding is deleted.
	assertAssetHoldingDates(t, db.db, test.AccountA, assetID,
		sql.NullBool{Valid: true, Bool: true},
		sql.NullInt64{Valid: true, Int64: 1},
		sql.NullInt64{Valid: true, Int64: 1})
}

func TestDestroyAssetDeleteCreatorsHolding(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetID := uint64(1)

	// Create an asset. Create a transaction where all special addresses are different
	// from creator's address.
	txn0 := transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "acfg",
				Header: transactions.Header{
					Sender: test.AccountA,
					GenesisHash: test.GenesisHash,
				},
				AssetConfigTxnFields: transactions.AssetConfigTxnFields{
					AssetParams: basics.AssetParams{
						Manager:  test.AccountB,
						Reserve:  test.AccountB,
						Freeze:   test.AccountB,
						Clawback: test.AccountB,
					},
				},
			},
		},
	}

	// Another account opts in.
	txn1 := test.MakeAssetOptInTxn(assetID, test.AccountC)

	// Destroy an asset.
	txn2 := test.MakeAssetDestroyTxn(assetID, test.AccountB)

	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &txn0, &txn1, &txn2)
	require.NoError(t, err)
	err = db.AddBlock(block)
	require.NoError(t, err)

	// Check that the creator's asset holding is deleted.
	assertAssetHoldingDates(t, db.db, test.AccountA, assetID,
		sql.NullBool{Valid: true, Bool: true},
		sql.NullInt64{Valid: true, Int64: 1},
		sql.NullInt64{Valid: true, Int64: 1})

	// Check that other account's asset holding was not deleted.
	assertAssetHoldingDates(t, db.db, test.AccountC, assetID,
		sql.NullBool{Valid: true, Bool: false},
		sql.NullInt64{Valid: true, Int64: 1},
		sql.NullInt64{Valid: false, Int64: 0})

	// Check that the manager does not have an asset holding.
	count := queryInt(
		db.db, "SELECT COUNT(*) FROM account_asset WHERE addr = $1", test.AccountB[:])
	assert.Equal(t, 0, count)
}

// Test that block import adds the freeze/sender accounts to txn_participation.
func TestAssetFreezeTxnParticipation(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	///////////
	// Given // A block containing an asset freeze txn
	///////////

	// Create a block with freeze txn
	assetid := uint64(1)

	createAsset := test.MakeCreateAssetTxn(
		uint64(1000000), uint64(6), false, "mcn", "my coin", "http://antarctica.com",
		test.AccountA)
	optinB := test.MakeAssetOptInTxn(assetid, test.AccountB)
	freeze := test.MakeAssetFreezeTxn(assetid, true, test.AccountA, test.AccountB)

	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &createAsset, &optinB, &freeze)
	require.NoError(t, err)

	//////////
	// When // We import the block.
	//////////
	err = db.AddBlock(block)
	require.NoError(t, err)

	//////////
	// Then // Both accounts should have an entry in the txn_participation table.
	//////////
	round := uint64(1)
	intra := uint64(2)

	query :=
		"SELECT COUNT(*) FROM txn_participation WHERE addr = $1 AND round = $2 AND " +
			"intra = $3"
	acctACount := queryInt(db.db, query, test.AccountA[:], round, intra)
	acctBCount := queryInt(db.db, query, test.AccountB[:], round, intra)
	assert.Equal(t, 1, acctACount)
	assert.Equal(t, 1, acctBCount)
}

// TestAddBlockGenesis tests that adding block 0 is successful.
func TestAddBlockGenesis(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	opts := idb.GetBlockOptions{
		Transactions: true,
	}
	blockHeaderRet, txns, err := db.GetBlock(context.Background(), 0, opts)
	require.NoError(t, err)
	assert.Empty(t, txns)
	assert.Equal(t, test.MakeGenesisBlock().BlockHeader, blockHeaderRet)

	maxRound, err := db.GetMaxRoundAccounted()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), maxRound)
}

// TestAddBlockAssetCloseAmountInTxnExtra tests that we set the correct asset close
// amount in `txn.extra` column.
func TestAddBlockAssetCloseAmountInTxnExtra(t *testing.T) {
	db, shutdownFunc := setupIdb(t, test.MakeGenesis(), test.MakeGenesisBlock())
	defer shutdownFunc()

	assetid := uint64(1)

	createAsset := test.MakeCreateAssetTxn(
		uint64(1000000), uint64(6), false, "mcn", "my coin", "http://antarctica.com",
		test.AccountA)
	optinB := test.MakeAssetOptInTxn(assetid, test.AccountB)
	transferAB := test.MakeAssetTransferTxn(
		assetid, 100, test.AccountA, test.AccountB, basics.Address{})
	optinC := test.MakeAssetOptInTxn(assetid, test.AccountC)
	// Close B to C.
	closeB := test.MakeAssetTransferTxn(
		assetid, 30, test.AccountB, test.AccountA, test.AccountC)

	block, err := test.MakeBlockForTxns(
		test.MakeGenesisBlock().BlockHeader, &createAsset, &optinB, &transferAB,
		&optinC, &closeB)
	require.NoError(t, err)

	err = db.AddBlock(block)
	require.NoError(t, err)

	// Check asset close amount in the `closeB` transaction.
	round := uint64(1)
	intra := uint64(4)

	tf := idb.TransactionFilter{
		Round:  &round,
		Offset: &intra,
	}
	rowsCh, _ := db.Transactions(context.Background(), tf)

	row, ok := <-rowsCh
	require.True(t, ok)
	require.NoError(t, row.Error)
	assert.Equal(t, uint64(70), row.Extra.AssetCloseAmount)

	row, ok = <-rowsCh
	require.False(t, ok)
}

func TestAddBlockIncrementsMaxRoundAccounted(t *testing.T) {
	_, connStr, shutdownFunc := setupPostgres(t)
	defer shutdownFunc()
	db, err := OpenPostgres(connStr, idb.IndexerDbOptions{}, nil)
	assert.NoError(t, err)

	db.LoadGenesis(test.MakeGenesis())

	round, err := db.GetMaxRoundAccounted()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), round)

	block := test.MakeGenesisBlock()
	db.AddBlock(block)

	round, err = db.GetMaxRoundAccounted()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), round)

	block, err = test.MakeBlockForTxns(block.BlockHeader)
	require.NoError(t, err)
	db.AddBlock(block)

	round, err = db.GetMaxRoundAccounted()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), round)

	block, err = test.MakeBlockForTxns(block.BlockHeader)
	require.NoError(t, err)
	db.AddBlock(block)

	round, err = db.GetMaxRoundAccounted()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), round)
}

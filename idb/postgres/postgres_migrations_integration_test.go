package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/protocol"
	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/postgres/internal/encoding"
	"github.com/algorand/indexer/idb/postgres/internal/schema"
	pgtest "github.com/algorand/indexer/idb/postgres/internal/testing"
	"github.com/algorand/indexer/util/test"
)

func nextMigrationNum(t *testing.T, db *IndexerDb) int {
	j, err := db.getMetastate(context.Background(), nil, schema.MigrationMetastateKey)
	assert.NoError(t, err)

	assert.True(t, len(j) > 0)

	var state MigrationState
	err = encoding.DecodeJSON([]byte(j), &state)
	assert.NoError(t, err)

	return state.NextMigration
}

func TestDeleteUnaccountedData(t *testing.T) {
	_, connStr, shutdownFunc := pgtest.SetupPostgres(t)
	defer shutdownFunc()
	db, _, err := OpenPostgres(connStr, idb.IndexerDbOptions{}, nil)
	assert.NoError(t, err)

	migrationState := MigrationState{NextMigration: 10}

	// Insert data.
	err = upsertMigrationState(db, nil, &migrationState)
	require.NoError(t, err)
	{
		nextRound := uint64(4)
		importState := importState{NextRoundToAccount: &nextRound}
		err = db.setImportState(nil, importState)
		require.NoError(t, err)
	}
	{
		stxnad := test.MakePaymentTxn(
			0, 0, 0, 0, 0, 0, test.AccountA, test.AccountB, basics.Address{}, basics.Address{})
		txnbytes := protocol.Encode(&stxnad)
		queryTxn := `INSERT INTO txn
			(round, intra, typeenum, asset, txid, txnbytes, txn, extra)
			VALUES ($1, 0, 0, 0, '', $2, '{}', '{}')`
		queryTxnPart := `INSERT INTO txn_participation
			(addr, round, intra) VALUES ($1, $2, 0)`
		for i := 3; i <= 5; i++ {
			_, err = db.db.Exec(context.Background(), queryTxn, i, txnbytes)
			require.NoError(t, err)
			_, err = db.db.Exec(context.Background(), queryTxnPart, test.AccountA[:], i)
			require.NoError(t, err)
		}
	}

	// Run migration.
	err = DeleteUnaccountedData(db, &migrationState)
	require.NoError(t, err)

	// Query data.
	{
		query := `SELECT round FROM txn`
		rows, err := db.db.Query(context.Background(), query)
		require.NoError(t, err)

		require.True(t, rows.Next())
		var round uint64
		err = rows.Scan(&round)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), round)
		assert.False(t, rows.Next())
	}
	{
		query := `SELECT round FROM txn_participation`
		rows, err := db.db.Query(context.Background(), query)
		require.NoError(t, err)

		require.True(t, rows.Next())
		var round uint64
		err = rows.Scan(&round)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), round)
		assert.False(t, rows.Next())
	}

	// Check the next migration number.
	assert.Equal(t, 11, migrationState.NextMigration)
	newNum := nextMigrationNum(t, db)
	assert.Equal(t, 11, newNum)
}

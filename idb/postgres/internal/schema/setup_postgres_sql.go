// Code generated from source setup_postgres.sql via go generate. DO NOT EDIT.

package schema

const SetupPostgresSql = `-- This file is setup_postgres.sql which gets compiled into go source using a go:generate statement in postgres.go
--
-- TODO? replace all 'addr bytea' with 'addr_id bigint' and a mapping table? makes addrs an 8 byte int that fits in a register instead of a 32 byte string

CREATE TABLE IF NOT EXISTS block_header (
round bigint PRIMARY KEY,
realtime timestamp without time zone NOT NULL,
rewardslevel bigint NOT NULL,
header jsonb NOT NULL
);

-- For looking round by timestamp. We could replace this with a round-to-timestamp algorithm, it should be extremely
-- efficient since there is such a high correlation between round and time.
CREATE INDEX IF NOT EXISTS block_header_time ON block_header (realtime);

CREATE TABLE IF NOT EXISTS txn (
round bigint NOT NULL,
intra smallint NOT NULL,
typeenum smallint NOT NULL,
asset bigint NOT NULL, -- 0=Algos, otherwise AssetIndex
txid bytea NOT NULL, -- base32 of [32]byte hash
txnbytes bytea NOT NULL, -- msgpack encoding of signed txn with apply data
txn jsonb NOT NULL, -- json encoding of signed txn with apply data
extra jsonb,
PRIMARY KEY ( round, intra )
);

-- For transaction lookup
CREATE INDEX IF NOT EXISTS txn_by_tixid ON txn ( txid );

-- Optional, to make txn queries by asset fast:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS txn_asset ON txn (asset, round, intra);

CREATE TABLE IF NOT EXISTS txn_participation (
addr bytea NOT NULL,
round bigint NOT NULL,
intra smallint NOT NULL
);

-- For query account transactions
CREATE UNIQUE INDEX IF NOT EXISTS txn_participation_i ON txn_participation ( addr, round DESC, intra DESC );

-- expand data.basics.AccountData
CREATE TABLE IF NOT EXISTS account (
  addr bytea primary key,
  microalgos bigint NOT NULL, -- okay because less than 2^54 Algos
  rewardsbase bigint NOT NULL,
  rewards_total bigint NOT NULL,
  deleted bool NOT NULL, -- whether or not it is currently deleted
  created_at bigint NOT NULL DEFAULT 0, -- round that the account is first used
  closed_at bigint, -- round that the account was last closed
  keytype varchar(8), -- sig,msig,lsig
  account_data jsonb -- trimmed AccountData that only contains auth addr and keyreg info
);

-- data.basics.AccountData Assets[asset id] AssetHolding{}
CREATE TABLE IF NOT EXISTS account_asset (
  addr bytea NOT NULL, -- [32]byte
  assetid bigint NOT NULL,
  amount numeric(20) NOT NULL, -- need the full 18446744073709551615
  frozen boolean NOT NULL,
  deleted bool NOT NULL, -- whether or not it is currently deleted
  created_at bigint NOT NULL DEFAULT 0, -- round that the asset was added to an account
  closed_at bigint, -- round that the asset was last removed from the account
  PRIMARY KEY (addr, assetid)
);

-- For account lookup
CREATE INDEX IF NOT EXISTS account_asset_by_addr ON account_asset ( addr );

-- Optional, to make queries of all asset balances fast /v2/assets/<assetid>/balances
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS account_asset_asset ON account_asset (assetid, addr ASC);

-- data.basics.AccountData AssetParams[index] AssetParams{}
CREATE TABLE IF NOT EXISTS asset (
  index bigint PRIMARY KEY,
  creator_addr bytea NOT NULL,
  params jsonb NOT NULL, -- data.basics.AssetParams -- TODO index some fields?
  deleted bool NOT NULL, -- whether or not it is currently deleted
  created_at bigint NOT NULL DEFAULT 0, -- round that the asset was created
  closed_at bigint -- round that the asset was closed; cannot be recreated because the index is unique
);

-- For account lookup
CREATE INDEX IF NOT EXISTS asset_by_creator_addr ON asset ( creator_addr );

-- subsumes ledger/accountdb.go accounttotals and acctrounds
-- "state":{online, onlinerewardunits, offline, offlinerewardunits, notparticipating, notparticipatingrewardunits, rewardslevel, round bigint}
CREATE TABLE IF NOT EXISTS metastate (
  k text primary key,
  v jsonb
);

-- per app global state
-- roughly go-algorand/data/basics/userBalance.go AppParams
CREATE TABLE IF NOT EXISTS app (
  index bigint PRIMARY KEY,
  creator bytea, -- account address
  params jsonb,
  deleted bool NOT NULL, -- whether or not it is currently deleted
  created_at bigint NOT NULL DEFAULT 0, -- round that the asset was created
  closed_at bigint -- round that the app was deleted; cannot be recreated because the index is unique
);

-- For account lookup
CREATE INDEX IF NOT EXISTS app_by_creator ON app ( creator );

-- per-account app local state
CREATE TABLE IF NOT EXISTS account_app (
  addr bytea,
  app bigint,
  localstate jsonb,
  deleted bool NOT NULL, -- whether or not it is currently deleted
  created_at bigint NOT NULL DEFAULT 0, -- round that the app was added to an account
  closed_at bigint, -- round that the account_app was last removed from the account
  PRIMARY KEY (addr, app)
);

-- For account lookup
CREATE INDEX IF NOT EXISTS account_app_by_addr ON account_app ( addr );
`

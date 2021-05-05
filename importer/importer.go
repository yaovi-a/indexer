package importer

import (
	"fmt"

	"github.com/algorand/go-algorand/config"
	"github.com/algorand/go-algorand/rpcs"

	"github.com/algorand/indexer/idb"
)

type Importer struct {
	db idb.IndexerDb
}

// ImportBlock processes a block and adds it to the IndexerDb
func (imp *Importer) ImportBlock(blockContainer *rpcs.EncodedBlockCert) error {
	block := &blockContainer.Block

	proto, ok := config.Consensus[block.CurrentProtocol]
	if !ok {
		return fmt.Errorf("protocol %s not found", block.CurrentProtocol)
	}

	for intra := range block.Payset {
		stxn := &block.Payset[intra]

		if stxn.HasGenesisID {
			stxn.Txn.GenesisID = block.GenesisID()
		}
		if stxn.HasGenesisHash || proto.RequireGenesisHash {
			stxn.Txn.GenesisHash = block.GenesisHash()
		}
	}

	return imp.db.AddBlock(blockContainer.Block)
}

// NewDBImporter creates a new importer object.
func NewImporter(db idb.IndexerDb) Importer {
	return Importer{db: db}
}

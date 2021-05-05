package test

import (
	"fmt"

	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/protocol"
)

var (
	// AccountA is a premade account for use in tests.
	AccountA = DecodeAddressOrPanic("GJR76Q6OXNZ2CYIVCFCDTJRBAAR6TYEJJENEII3G2U3JH546SPBQA62IFY")
	// AccountB is a premade account for use in tests.
	AccountB = DecodeAddressOrPanic("N5T74SANUWLHI6ZWYFQBEB6J2VXBTYUYZNWQB2V26DCF4ARKC7GDUW3IRU")
	// AccountC is a premade account for use in tests.
	AccountC = DecodeAddressOrPanic("OKUWMFFEKF4B4D7FRQYBVV3C2SNS54ZO4WZ2MJ3576UYKFDHM5P3AFMRWE")
	// AccountD is a premade account for use in tests.
	AccountD = DecodeAddressOrPanic("6TB2ZQA2GEEDH6XTIOH5A7FUSGINXDPW5ONN6XBOBBGGUXVHRQTITAIIVI")
	// AccountE is a premade account for use in tests.
	AccountE = DecodeAddressOrPanic("QYE3RIRIIUS4VRZ4WYR7E5R6WBHTQXUY7F62C7U77SSRAXUSFTSRQPXPPU")
	// FeeAddr is the fee addess to use when creating the state object.
	FeeAddr = DecodeAddressOrPanic("ZROKLZW4GVOK5WQIF2GUR6LHFVEZBMV56BIQEQD4OTIZL2BPSYYUKFBSHM")
	// RewardAddr is the fee addess to use when creating the state object.
	RewardAddr = DecodeAddressOrPanic("4C3S3A5II6AYMEADSW7EVL7JAKVU2ASJMMJAGVUROIJHYMS6B24NCXVEWM")

	GenesisHash crypto.Digest

	// Proto is a fake protocol version.
	Proto = protocol.ConsensusFuture
)

func init() {
	GenesisHash[0] = 77;
}

// DecodeAddressOrPanic is a helper to ensure addresses are initialized.
func DecodeAddressOrPanic(addr string) basics.Address {
	if addr == "" {
		return basics.Address{}
	}

	result, err := basics.UnmarshalChecksumAddress(addr)
	if err != nil {
		panic(fmt.Sprintf("Failed to decode address: '%s'", addr))
	}
	return result
}

// MakeAssetConfigOrPanic is a helper to ensure test asset config are initialized.
func MakeAssetConfigTxn(total, decimals uint64, defaultFrozen bool, unitName, assetName, url string, addr basics.Address) transactions.SignedTxnWithAD {
	return transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "acfg",
				Header: transactions.Header{
					Sender:     addr,
					Fee:        basics.MicroAlgos{Raw: 1000},
				},
				AssetConfigTxnFields: transactions.AssetConfigTxnFields{
					AssetParams: basics.AssetParams{
						Total:         total,
						Decimals:      uint32(decimals),
						DefaultFrozen: defaultFrozen,
						UnitName:      unitName,
						AssetName:     assetName,
						URL:           url,
						MetadataHash:  [32]byte{},
						Manager:       addr,
						Reserve:       addr,
						Freeze:        addr,
						Clawback:      addr,
					},
				},
			},
		},
	}
}

// MakeAssetFreezeOrPanic create an asset freeze/unfreeze transaction.
func MakeAssetFreezeTxn(assetid uint64, frozen bool, sender, freezeAccount basics.Address) transactions.SignedTxnWithAD {
	return transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "afrz",
				Header: transactions.Header{
					Sender:     sender,
					Fee:        basics.MicroAlgos{Raw: 1000},
				},
				AssetFreezeTxnFields: transactions.AssetFreezeTxnFields{
					FreezeAccount: freezeAccount,
					FreezeAsset:   basics.AssetIndex(assetid),
					AssetFrozen:   frozen,
				},
			},
		},
	}
}

// MakeAssetTxnOrPanic creates an asset transfer transaction.
func MakeAssetTransferTxn(assetid, amt, closeAmt uint64, sender, receiver, close basics.Address) transactions.SignedTxnWithAD {
	return transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "axfer",
				Header: transactions.Header{
					Sender:     sender,
					Fee:        basics.MicroAlgos{Raw: 1000},
				},
				AssetTransferTxnFields: transactions.AssetTransferTxnFields{
					XferAsset:   basics.AssetIndex(assetid),
					AssetAmount: amt,
					//only used for clawback transactions
					//AssetSender:   basics.Address{},
					AssetReceiver: receiver,
					AssetCloseTo:  close,
				},
			},
		},
		ApplyData: transactions.ApplyData{
			AssetClosingAmount: closeAmt,
		},
	}
}

func MakeAssetOptInTxn(assetid uint64, address basics.Address) transactions.SignedTxnWithAD {
	return MakeAssetTransferTxn(assetid, 0, 0, address, address, basics.Address{})
}

// MakeAssetDestroyTxn makes a transaction that destroys an asset.
func MakeAssetDestroyTxn(assetID uint64) transactions.SignedTxnWithAD {
	return transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "acfg",
				AssetConfigTxnFields: transactions.AssetConfigTxnFields{
					ConfigAsset: basics.AssetIndex(assetID),
				},
			},
		},
	}
}

// MakePayTxnRowOrPanic creates an algo transfer transaction.
func MakePaymentTxn(fee, amt, closeAmt, sendRewards, receiveRewards,
	closeRewards uint64, sender, receiver, close, rekeyTo basics.Address) transactions.SignedTxnWithAD {
	return transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "pay",
				Header: transactions.Header{
					Sender:     sender,
					Fee:        basics.MicroAlgos{Raw: fee},
					RekeyTo:    rekeyTo,
				},
				PaymentTxnFields: transactions.PaymentTxnFields{
					Receiver:         receiver,
					Amount:           basics.MicroAlgos{Raw: amt},
					CloseRemainderTo: close,
				},
			},
		},
		ApplyData: transactions.ApplyData{
			ClosingAmount:   basics.MicroAlgos{Raw: closeAmt},
			SenderRewards:   basics.MicroAlgos{Raw: sendRewards},
			ReceiverRewards: basics.MicroAlgos{Raw: receiveRewards},
			CloseRewards:    basics.MicroAlgos{Raw: closeRewards},
		},
	}
}

// MakeSimpleKeyregOnlineTxn creates a fake key registration transaction.
func MakeSimpleKeyregOnlineTxn(sender basics.Address) transactions.SignedTxnWithAD {
	var votePK crypto.OneTimeSignatureVerifier
	votePK[0] = 1

	var selectionPK crypto.VRFVerifier
	selectionPK[0] = 2

	return transactions.SignedTxnWithAD{
		SignedTxn: transactions.SignedTxn{
			Txn: transactions.Transaction{
				Type: "keyreg",
				Header: transactions.Header{
					Sender:     sender,
				},
				KeyregTxnFields: transactions.KeyregTxnFields{
					VotePK:          votePK,
					SelectionPK:     selectionPK,
					VoteKeyDilution: 1,
				},
			},
		},
	}
}

// MakeBlockForTxns takes some transactions and constructs a block compatible with the indexer import function.
func MakeBlockForTxns(prevHeader bookkeeping.BlockHeader, inputs ...*transactions.SignedTxnWithAD) bookkeeping.Block {
	var txns []transactions.SignedTxnInBlock

	for _, txn := range inputs {
		txns = append(txns, transactions.SignedTxnInBlock{
			SignedTxnWithAD: transactions.SignedTxnWithAD{SignedTxn: txn.SignedTxn},
			HasGenesisID:    true,
		})
	}

	return bookkeeping.Block{
		BlockHeader: bookkeeping.BlockHeader{
			Round: basics.Round(prevHeader.Round + 1),
			Branch: prevHeader.Hash(),
			GenesisID: MakeGenesis().ID(),
			GenesisHash: GenesisHash,
			RewardsState: bookkeeping.RewardsState{
				FeeSink: FeeAddr,
				RewardsPool: RewardAddr,
			},
			UpgradeState: bookkeeping.UpgradeState{CurrentProtocol: Proto},
			TxnCounter: prevHeader.TxnCounter + uint64(len(inputs)),
		},
		Payset: txns,
	}
}

func MakeGenesis() bookkeeping.Genesis {
	return bookkeeping.Genesis{
		SchemaID: "main",
		Network: "mynet",
		Proto: Proto,
		Allocation: []bookkeeping.GenesisAllocation{
			{
				Address: AccountA.String(),
				State: basics.AccountData{
					MicroAlgos: basics.MicroAlgos{Raw: 1000 * 1000 * 1000 * 1000},
				},
			},
			{
				Address: AccountB.String(),
				State: basics.AccountData{
					MicroAlgos: basics.MicroAlgos{Raw: 1000 * 1000 * 1000 * 1000},
				},
			},
			{
				Address: AccountC.String(),
				State: basics.AccountData{
					MicroAlgos: basics.MicroAlgos{Raw: 1000 * 1000 * 1000 * 1000},
				},
			},
			{
				Address: AccountD.String(),
				State: basics.AccountData{
					MicroAlgos: basics.MicroAlgos{Raw: 1000 * 1000 * 1000 * 1000},
				},
			},
		},
		RewardsPool: RewardAddr.String(),
		FeeSink: FeeAddr.String(),
	}
}

func MakeGenesisBlock() bookkeeping.Block {
	return bookkeeping.Block{
		BlockHeader: bookkeeping.BlockHeader{
			GenesisID: MakeGenesis().ID(),
			GenesisHash: GenesisHash,
			RewardsState: bookkeeping.RewardsState{
				FeeSink: FeeAddr,
				RewardsPool: RewardAddr,
			},
			UpgradeState: bookkeeping.UpgradeState{CurrentProtocol: Proto},
		},
	}
}

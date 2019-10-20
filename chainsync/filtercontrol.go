package chainsync

import (
	"fmt"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
)

// ErrCheckpointMismatch is returned if given filter headers don't pass our
// control check.
var ErrCheckpointMismatch = fmt.Errorf("checkpoint doesn't match")

// filterHeaderCheckpoints holds a mapping from heights to filter headers for
// various heights. We use them to check whether peers are serving us the
// expected filter headers.
var filterHeaderCheckpoints = map[wire.BitcoinNet]map[uint32]*chainhash.Hash{
	// Mainnet filter header checkpoints.
	chaincfg.MainNetParams.Net: {
		100000: hashFromStr("075e4781d68abed9a923a0deb6bf2f73e9b5cdb15b7f1ff07b719bfa8b05de0f"),
		200000: hashFromStr("2e77f07befefcf07b7b8fd158c4dc3d28502f89667b921730ed4ff56dfa5da93"),
		300000: hashFromStr("b36d11b85d9cf49f974a71d8c0534223dc65fe3f3ed49479d81e4d89a4439d2a"),
		400000: hashFromStr("9b9c91f0e234418281506470dfecb3284c6863a00643e037481fe3fbc24242d4"),
		500000: hashFromStr("a90ee1fd88c0007747b1750f59b6325157857ded949cc91394d8aafada6d1358"),
		540000: hashFromStr("c87b13603861d20fc37679966513a306b785013aa8cfba71b79be8aa7453482e"),
	},

	// Testnet filter header checkpoints.
	chaincfg.TestNet3Params.Net: {
		100000:  hashFromStr("06be769fee8fee75dcc9c4165b1838fed8c8f780efb464cefe3a1e7eecb64603"),
		200000:  hashFromStr("1c8266e0f7fd7463f652f9c97841c7aa4150d845637104e8404a3246d6d45938"),
		400000:  hashFromStr("f6101ef9d252396045fac30ca2b8991866a08b20d9df5347af994ae1e3d0e463"),
		600000:  hashFromStr("e0dceedc20598d5b68f90782c59d0279a91e3d02016d70ae8d32d3195c316c2d"),
		800000:  hashFromStr("4d1749da2c71bdcb8d5c3315fbf53089ee57bb4a1c92f89353b099ab9e1cfb32"),
		1000000: hashFromStr("9e3b4677dd3f6371f6c1acdb2a32fc81ae24d51506ce6a430755153cde266933"),
		1200000: hashFromStr("49cb93219a1ce7360e4743528e61dc9c640cdb372b63087f269ce3be7fb465ee"),
		1300000: hashFromStr("c28ecc10a583bb6232f05ce29074ba4dece8d438cc37d66cbdd7464ff67ee448"),
	},
}

// ControlCFHeader controls the given filter header against our list of
// checkpoints. It returns ErrCheckpointMismatch if we have a checkpoint at the
// given height, and it doesn't match.
func ControlCFHeader(params chaincfg.Params, fType wire.FilterType,
	height uint32, filterHeader *chainhash.Hash) error {

	if fType != wire.GCSFilterRegular {
		return fmt.Errorf("unsupported filter type %v", fType)
	}

	control, ok := filterHeaderCheckpoints[params.Net]
	if !ok {
		return nil
	}

	hash, ok := control[height]
	if !ok {
		return nil
	}

	if *filterHeader != *hash {
		return ErrCheckpointMismatch
	}

	return nil
}

// hashFromStr makes a chainhash.Hash from a valid hex string. If the string is
// invalid, a nil pointer will be returned.
func hashFromStr(hexStr string) *chainhash.Hash {
	hash, _ := chainhash.NewHashFromStr(hexStr)
	return hash
}

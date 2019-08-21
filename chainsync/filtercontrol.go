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
		300000: hashFromStr("953bac345b184454f05d5e2a69e356994c03475fe3e2942d2343e4d4dd2ffc29"),
		400000: hashFromStr("f9e7a4b34557c0178efaee5d1bc6be39a379f69a68e65ed5ad9c6d8e29a55da4"),
		500000: hashFromStr("930253a771e66966933aef257413f4f7843bf9ec39b362db59f391045c5a145f"),
		540000: hashFromStr("cf75f29f2cb4d4b72f064eb55271906980dcc6631983d6345d76a42e4f447f85"),
	},

	// Testnet filter header checkpoints.
	chaincfg.TestNet3Params.Net: {
		100000:  hashFromStr("56bc58d45394f3d45bac5f20005fa8fd194067964c80ccf4ebe6cfffa1b3d61d"),
		200000:  hashFromStr("68c3017878f08ae63ee9b6c28ae89e394fea40859123fb22b239cd6c00b1c2ef"),
		400000:  hashFromStr("40bdeaabc3d30db1ca004b02d4a7fa0f963610716d0d0561880a7548e13f568e"),
		600000:  hashFromStr("042c58edfd9ceddb39164ebea2b82f9df80ae933693c7ee2a0a28f1e6dcd2bb9"),
		800000:  hashFromStr("8b0a709023e38aa87921f0eee9a4a0237d9f81ef4b946befafd14c215b615e58"),
		1000000: hashFromStr("a06d3e4bbecb469286692ad90f78ab9f7d05b5a84d3182f662d7e114e32d3a8d"),
		1200000: hashFromStr("04f60385070fe797effd8c06623e4817d462c2b581ffe5770f15991f81fff07e"),
		1300000: hashFromStr("62bfb89867a586d014be35d49992f846951de8481fc4e22ef1e3a767af7f2a4d"),
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

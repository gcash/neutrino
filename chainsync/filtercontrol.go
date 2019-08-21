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
		100000:  hashFromStr("97c0633f14625627fcd133250ad8cc525937e776b5f3fd272b06d02c58b65a1c"),
		200000:  hashFromStr("51aa817e5abe3acdcf103616b1a5736caf84bc3773a7286e9081108ecc38cc87"),
		400000:  hashFromStr("4aab9b3d4312cd85cfcd48a08b36c4402bfdc1e8395dcf4236c3029dfa837c48"),
		600000:  hashFromStr("713d9c9198e2dba0739e85aab6875cb951c36297b95a2d51131aa6919753b55d"),
		800000:  hashFromStr("0dafdff27269a70293c120b14b1f5e9a72a5e8688098cfc6140b9d64f8325b99"),
		1000000: hashFromStr("c2043fa2f6eb5f8f8d2c5584f743187f36302ed86b62c302e31155f378da9c5f"),
		1390000: hashFromStr("ec71c508c02f59b2af2f34b64dfd79ffba55d9ef7d00589b0a2c3178da89e4c0"),
		1400000: hashFromStr("f9ae1750483d4c8ce82512616b1ded932886af46decb8d3e575907930542d9b3"),
		1500000: hashFromStr("dc0cfa13daf09df9b8dbe7532f75ebdb4255860b295016b2ca4b789394bc5090"),
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

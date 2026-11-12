package neutrino

import (
	"fmt"
	"time"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/neutrino/filterdb"
	"github.com/gcash/neutrino/headerfs"
)

// A ChainService syncs the filters of one type: the regular filter, or the
// script filter (filter type 1), which lists the scripts of the outputs a
// block creates and spends and the token categories of both, so that a wallet
// watches only its own scripts and categories.  The two differ in how a peer
// serves a filter, and in how a disagreement among peers is settled.

// headerTypeOf returns the filter header store's type for a filter type.
func headerTypeOf(fType wire.FilterType) (headerfs.HeaderType, error) {
	switch fType {
	case wire.GCSFilterRegular:
		return headerfs.RegularFilter, nil
	case wire.GCSFilterScripts:
		return headerfs.ScriptFilter, nil
	default:
		return 0, fmt.Errorf("unknown filter type: %v", fType)
	}
}

// dbFilterTypeOf returns the filter database's type for a filter type.
func dbFilterTypeOf(fType wire.FilterType) filterdb.FilterType {
	if fType == wire.GCSFilterScripts {
		return filterdb.ScriptFilter
	}
	return filterdb.RegularFilter
}

// parseServedFilter parses a filter as a cfilter message carries it.  A
// script filter comes with the root of its block's entry tree in front, which
// is returned too; a regular filter has none.
func parseServedFilter(fType wire.FilterType, data []byte) (*gcs.Filter, chainhash.Hash, error) {
	switch fType {
	case wire.GCSFilterRegular:
		filter, err := gcs.FromNBytes(builder.DefaultP, builder.DefaultM, data)
		return filter, chainhash.Hash{}, err
	case wire.GCSFilterScripts:
		return builder.ParseScriptFilter(data)
	default:
		return nil, chainhash.Hash{}, fmt.Errorf("unknown filter type: %v", fType)
	}
}

// servedFilterHash returns the hash of a filter as it is served, which
// cfheaders messages list: for a script filter, the root is part of it.
func servedFilterHash(fType wire.FilterType, filter *gcs.Filter,
	root chainhash.Hash) (chainhash.Hash, error) {

	if fType == wire.GCSFilterScripts {
		return builder.GetScriptFilterHash(filter, root)
	}
	return builder.GetFilterHash(filter)
}

// servedFilterHeader returns the filter header of a filter as it is served,
// after the previous block's filter header.
func servedFilterHeader(fType wire.FilterType, filter *gcs.Filter,
	root, prevHeader chainhash.Hash) (chainhash.Hash, error) {

	if fType == wire.GCSFilterScripts {
		return builder.MakeHeaderForScriptFilter(filter, root, prevHeader)
	}
	return builder.MakeHeaderForFilter(filter, prevHeader)
}

// scriptProofTimeout is how long a peer has to answer a getcfproof.
const scriptProofTimeout = 10 * time.Second

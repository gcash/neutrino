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

// fetchScriptProof asks one peer for the cfproof of a leaf of a block's
// script filter entry tree.  It returns nil when the peer is gone or doesn't
// answer in time.
func (s *ChainService) fetchScriptProof(peer string, blockHash chainhash.Hash,
	index uint32, quit <-chan struct{}) *wire.MsgCFProof {

	sp := s.PeerByAddr(peer)
	if sp == nil || !sp.Connected() {
		return nil
	}

	msgChan := make(chan spMsg)
	subQuit := make(chan struct{})
	subscription := spMsgSubscription{msgChan: msgChan, quitChan: subQuit}
	sp.subscribeRecvMsg(subscription)
	defer func() {
		sp.unsubscribeRecvMsgs(subscription)
		close(subQuit)
	}()

	sp.QueueMessage(
		wire.NewMsgGetCFProof(wire.GCSFilterScripts, &blockHash, index), nil,
	)

	timeout := time.After(scriptProofTimeout)
	for {
		select {
		case m := <-msgChan:
			proof, ok := m.msg.(*wire.MsgCFProof)
			if ok && proof.BlockHash == blockHash && proof.LeafIndex == index {
				return proof
			}

		case <-timeout:
			log.Debugf("Peer %v didn't prove leaf %d of block %v in time",
				peer, index, blockHash)
			return nil

		case <-quit:
			return nil
		}
	}
}

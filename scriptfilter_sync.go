package neutrino

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"time"

	"github.com/aead/siphash"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/kkdai/bstream"
)

// servedFilter is a filter a peer served for a block: the filter, and for a
// script filter the root of the block's entry tree that came in front of it.
type servedFilter struct {
	filter *gcs.Filter
	root   chainhash.Hash
}

// askPeer sends a request to one peer and returns the first message from it
// that answers, or nil when the peer is gone, doesn't answer in time, or we
// are shutting down.
func (s *ChainService) askPeer(peer string, request wire.Message,
	answers func(wire.Message) bool, timeout time.Duration,
	quit <-chan struct{}) wire.Message {

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

	sp.QueueMessage(request, nil)

	expired := time.After(timeout)
	for {
		select {
		case m := <-msgChan:
			if answers(m.msg) {
				return m.msg
			}

		case <-expired:
			log.Debugf("Peer %v didn't answer our %v in time", peer,
				request.Command())
			return nil

		case <-quit:
			return nil
		}
	}
}

// fetchScriptProof asks one peer for the cfproof of a leaf of a block's
// script filter entry tree.  It returns nil when the peer doesn't answer.
func (s *ChainService) fetchScriptProof(peer string, blockHash chainhash.Hash,
	index uint32, quit <-chan struct{}) *wire.MsgCFProof {

	msg := s.askPeer(
		peer, wire.NewMsgGetCFProof(wire.GCSFilterScripts, &blockHash, index),
		func(msg wire.Message) bool {
			proof, ok := msg.(*wire.MsgCFProof)
			return ok && proof.BlockHash == blockHash && proof.LeafIndex == index
		}, scriptProofTimeout, quit,
	)
	proof, _ := msg.(*wire.MsgCFProof)
	return proof
}

// fetchScriptRange asks one peer for the leaves of a block's script filter
// entry tree whose keys start with a prefix.  It returns nil when the peer
// doesn't answer.
func (s *ChainService) fetchScriptRange(peer string, blockHash chainhash.Hash,
	bits uint16, prefix [32]byte, quit <-chan struct{}) *wire.MsgCFRange {

	// A cfrange doesn't repeat the prefix it answers.  We ask a peer for
	// one range at a time, so the next cfrange for the block is the
	// answer, which is then checked against the prefix.
	msg := s.askPeer(
		peer, wire.NewMsgGetCFRange(wire.GCSFilterScripts, &blockHash, bits, prefix),
		func(msg wire.Message) bool {
			leaves, ok := msg.(*wire.MsgCFRange)
			return ok && leaves.BlockHash == blockHash
		}, scriptProofTimeout, quit,
	)
	leaves, _ := msg.(*wire.MsgCFRange)
	return leaves
}

// settleScriptFiltersOfPeers finds the peers at fault among those that serve
// the given script filters for a block, with the proofs and leaves they give
// when asked.
func (b *blockManager) settleScriptFiltersOfPeers(header *wire.BlockHeader,
	height uint32, filters map[string]servedScriptFilter) []string {

	blockHash := header.BlockHash()
	proofs := func(peer string, index uint32) *wire.MsgCFProof {
		return b.server.fetchScriptProof(peer, blockHash, index, b.quit)
	}
	ranges := func(peer string, bits uint16, prefix [32]byte) *wire.MsgCFRange {
		return b.server.fetchScriptRange(peer, blockHash, bits, prefix, b.quit)
	}
	headers := func(hash *chainhash.Hash) (*wire.BlockHeader, uint32, bool) {
		prevHeader, prevHeight, err := b.server.BlockHeaders.FetchHeader(hash)
		return prevHeader, prevHeight, err == nil
	}
	return settleScriptFilters(
		blockHash, header, height, filters, proofs, ranges, headers,
	)
}

// scriptFiltersOfPeers fetches the script filter of a block from every peer,
// and returns the filters of the peers for which backs returns true: those
// whose filter gives the filter hash or header they announced.  The others
// that announced one are returned as bad.
func (b *blockManager) scriptFiltersOfPeers(header *wire.BlockHeader, height uint32,
	peers []string, backs func(peer string, f servedFilter) bool) (
	map[string]servedScriptFilter, []string) {

	served := b.fetchServedFilters(height, header.BlockHash(), wire.GCSFilterScripts)

	var badPeers []string
	filters := make(map[string]servedScriptFilter, len(peers))
	for _, peer := range peers {
		f, ok := served[peer]
		if !ok {
			log.Warnf("Peer %v did not respond to filter request, "+
				"considering bad", peer)
			badPeers = append(badPeers, peer)
			continue
		}
		data, err := f.filter.NBytes()
		if err != nil || !backs(peer, f) {
			log.Warnf("Peer %v serving filters not consistent with "+
				"filter hashes, considering bad.", peer)
			badPeers = append(badPeers, peer)
			continue
		}
		filters[peer] = servedScriptFilter{root: f.root, filter: data}
	}
	return filters, badPeers
}

// detectBadScriptPeers finds which of the peers that announced different
// script filter hashes for a block serve a wrong filter.  A script filter
// lists the scripts of spent outputs, which the block doesn't hold, so the
// peers' proofs of what their entry trees hold settle it, not the block.
func (b *blockManager) detectBadScriptPeers(header *wire.BlockHeader, height uint32,
	claims map[string]chainhash.Hash) ([]string, error) {

	peers := make([]string, 0, len(claims))
	for peer := range claims {
		peers = append(peers, peer)
	}
	filters, badPeers := b.scriptFiltersOfPeers(header, height, peers,
		func(peer string, f servedFilter) bool {
			hash, err := servedFilterHash(wire.GCSFilterScripts, f.filter, f.root)
			return err == nil && hash == claims[peer]
		},
	)
	if len(badPeers) != 0 {
		return badPeers, nil
	}

	log.Warnf("Attempting to reconcile cfheader mismatch amongst %v peers "+
		"with their proofs", len(filters))

	return b.settleScriptFiltersOfPeers(header, height, filters), nil
}

// scriptFilterHeaderOfPeers returns the script filter header of a block that
// the peers can back: the header of the filter left once the peers that
// announced different headers were told apart by their proofs.  claims holds
// the header each peer announced, all after the same previous header.
func (b *blockManager) scriptFilterHeaderOfPeers(header *wire.BlockHeader,
	height uint32, prevHeader chainhash.Hash,
	claims map[string]chainhash.Hash) (chainhash.Hash, error) {

	peers := make([]string, 0, len(claims))
	for peer := range claims {
		peers = append(peers, peer)
	}
	filters, _ := b.scriptFiltersOfPeers(header, height, peers,
		func(peer string, f servedFilter) bool {
			filterHeader, err := servedFilterHeader(
				wire.GCSFilterScripts, f.filter, f.root, prevHeader,
			)
			return err == nil && filterHeader == claims[peer]
		},
	)
	for _, peer := range b.settleScriptFiltersOfPeers(header, height, filters) {
		delete(filters, peer)
	}

	// The peers left serve one filter, and so announced one header.
	for peer := range filters {
		return claims[peer], nil
	}
	return chainhash.Hash{}, fmt.Errorf("no peer could back its script "+
		"filter for block %v at height %d", header.BlockHash(), height)
}

// scriptFilterProbeTimeout is how long a new peer has to show that it serves
// script filters.
const scriptFilterProbeTimeout = 15 * time.Second

// probeScriptFilters checks that a new peer serves script filters, and
// disconnects it otherwise.  No service bit says which nodes keep them, and
// one that doesn't ignores requests for them, so we ask for the script
// filter header of the genesis block, which every node that keeps them has.
func (s *ChainService) probeScriptFilters(sp *ServerPeer) {
	genesis := *s.chainParams.GenesisHash
	msg := s.askPeer(
		sp.Addr(), wire.NewMsgGetCFHeaders(wire.GCSFilterScripts, 0, &genesis),
		func(msg wire.Message) bool {
			headers, ok := msg.(*wire.MsgCFHeaders)
			return ok && headers.FilterType == wire.GCSFilterScripts &&
				headers.StopHash == genesis
		}, scriptFilterProbeTimeout, s.quit,
	)
	if msg != nil || !sp.Connected() {
		return
	}
	select {
	case <-s.quit:
		return
	default:
	}

	log.Infof("Disconnecting peer %v, as it doesn't serve script filters", sp)
	sp.Disconnect()
}

// ErrScriptFilterLacksOutput is returned when a block's script filter lacks
// the script or the token category of one of the block's outputs.
var errScriptFilterLacksOutput = errors.New("script filter lacks an output " +
	"of its block")

// scriptFilterHoldsOutputs checks that a block's script filter holds the
// script and the token category of every output the block creates.  The
// scripts of the outputs it spends are not in the block, so they can't be
// checked this way.
func scriptFilterHoldsOutputs(filter *gcs.Filter, block *wire.MsgBlock) error {
	var keys [][32]byte
	for _, tx := range block.Transactions {
		for _, txOut := range tx.TxOut {
			if script := txOut.PkScript; len(script) > 0 && script[0] != 0x6a {
				keys = append(keys, builder.ScriptKey(script))
			}
			if !txOut.TokenData.IsEmpty() {
				keys = append(keys, builder.CategoryKey(txOut.TokenData.CategoryID))
			}
		}
	}
	if len(keys) == 0 {
		return nil
	}

	// Matching the keys one at a time would read the filter once for each,
	// so we read its values once and look each key's value up in them, as
	// gcs.Filter does to match one: a key's value is its SipHash-2-4 under
	// the first half of the block hash, scaled to N*M.
	values, err := gcsFilterValues(filter)
	if err != nil {
		return err
	}
	blockHash := block.BlockHash()
	key := builder.DeriveKey(&blockHash)
	modulus := uint64(filter.N()) * builder.DefaultM
	for i := range keys {
		value, _ := bits.Mul64(siphash.Sum64(keys[i][:], &key), modulus)
		at := sort.Search(len(values), func(j int) bool { return values[j] >= value })
		if at == len(values) || values[at] != value {
			return fmt.Errorf("%w %v: key %x", errScriptFilterLacksOutput,
				blockHash, keys[i])
		}
	}
	return nil
}

// gcsFilterValues returns the values a filter holds, in order: the sums of
// its Golomb-Rice coded differences.
func gcsFilterValues(filter *gcs.Filter) ([]uint64, error) {
	data, err := filter.Bytes()
	if err != nil {
		return nil, err
	}
	stream := bstream.NewBStreamReader(data)
	values := make([]uint64, 0, filter.N())
	var last uint64
	for range filter.N() {
		var quotient uint64
		for {
			bit, err := stream.ReadBit()
			if err != nil {
				return nil, err
			}
			if !bit {
				break
			}
			quotient++
		}
		remainder, err := stream.ReadBits(int(filter.P()))
		if err != nil {
			return nil, err
		}
		last += quotient<<filter.P() + remainder
		values = append(values, last)
	}
	return values, nil
}

// verifyScriptBlockFilter checks a downloaded block against the script filter
// synced for it.  On a mismatch a repair of the filter headers is requested,
// which the peers' proofs settle, and ErrBlockFilterMismatch is returned.
func (s *ChainService) verifyScriptBlockFilter(block *bchutil.Block,
	height uint32) error {

	// Without the filter there is nothing to check the block against.  It
	// is in the cache when the block was fetched because it matched.
	filter, err := s.GetCFilter(*block.Hash(), wire.GCSFilterScripts)
	if err != nil || filter == nil {
		return nil
	}
	err = scriptFilterHoldsOutputs(filter, block.MsgBlock())
	if err == nil {
		return nil
	}
	if !errors.Is(err, errScriptFilterLacksOutput) {
		return err
	}

	// The filter read above belongs to another block if a reorg replaced
	// this one in the meantime.
	header, headerErr := s.BlockHeaders.FetchHeaderByHeight(height)
	if headerErr != nil || header.BlockHash() != *block.Hash() {
		return nil
	}

	log.Warnf("The script filter synced for block %v at height %d doesn't "+
		"hold its outputs: %v", block.Hash(), height, err)

	s.blockManager.requestFilterHeaderRepair()

	return fmt.Errorf("%w: block %v at height %d", ErrBlockFilterMismatch,
		block.Hash(), height)
}

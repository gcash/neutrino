package neutrino

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/neutrino/banman"
)

// A wallet on a reusable payment address is paid by a transaction one of
// whose first 30 inputs the sender ground until the first bits of its double
// SHA-256 equal bits of the wallet's scan key, its prefix.  The input prefix
// filter (filter type 2) of a block holds the first 16 bits of that hash for
// each such input of the block, so a wallet finds the blocks that may pay it
// by matching its prefix against the filters, which every peer serves alike,
// and downloads only those.  No request names the prefix.
//
// The filters are taken in block mode only.  For each run of blocks the
// peers that serve type 2 give the hashes of their filters; where they
// disagree, the block is downloaded, its filter is built from it, and the
// peers that gave another hash are wrong.  The filters are then fetched from
// one peer and checked against those hashes.  A block whose filter matches is
// downloaded, its filter is built from it and compared with the one served,
// which shows a peer wrong if they differ, and its inputs with the prefix are
// handed to the caller.

// RPAPrefix is a prefix of the first bits of an input's value: Value holds
// the first Bits bits, 1 to 16, of the 16-bit value.
type RPAPrefix struct {
	Value uint16
	Bits  uint8
}

// RPAInput is an input whose value has one of the prefixes searched for: the
// index of its transaction in the block, its own index, and its value.
type RPAInput struct {
	TxIndex int
	Index   int
	Value   uint16
}

// RPAMatch is a block with inputs whose values have one of the prefixes
// searched for.
type RPAMatch struct {
	Height int32
	Block  *bchutil.Block
	Inputs []RPAInput
}

var (
	// ErrNoRPAFilterPeers is returned when no peer gives the input prefix
	// filters of a run of blocks that the blocks don't show wrong.
	ErrNoRPAFilterPeers = errors.New("no peer serves the input prefix " +
		"filters of the blocks")

	// ErrRPAFilterMismatch is returned when the input prefix filter every
	// peer agreed on is not the one its block gives.
	ErrRPAFilterMismatch = errors.New("the input prefix filter the peers " +
		"serve is not the one its block gives")

	// ErrRPAPrefix is returned for a prefix of no bits, of more than 16, or
	// with a bit set past its length.
	ErrRPAPrefix = errors.New("an input prefix is 1 to 16 bits")
)

// rpaFilterBatch is how many blocks' filters are asked for at once: as many
// as one getcfilters can ask for.
var rpaFilterBatch int32 = wire.MaxGetCFiltersReqRange

// rpaFilterTimeout is how long a peer has to answer for the filters or
// filter hashes of a run of blocks.
const rpaFilterTimeout = 20 * time.Second

// rpaSource is what a search reads: the blocks and filters of the peers that
// serve them.
type rpaSource interface {
	// blockHash returns the hash of the block at a height of our chain.
	blockHash(height int32) (chainhash.Hash, error)

	// filterHashes asks every peer for the filter hashes of the blocks
	// from start up to the one with stop's hash, and returns the answers
	// by peer.  A peer that doesn't answer is left out.
	filterHashes(start int32, stop chainhash.Hash) map[string]*wire.MsgCFHeaders

	// filters asks a peer for the filters of count blocks from start up to
	// the one with stop's hash, and returns them in the order they came,
	// or nil when the peer doesn't give as many.
	filters(peer string, start int32, stop chainhash.Hash, count int) []*wire.MsgCFilter

	// block downloads a block.
	block(hash chainhash.Hash) (*bchutil.Block, error)

	// bad drops a peer shown to serve a wrong filter.
	bad(peer string)
}

// checkRPAPrefixes checks that each prefix has 1 to 16 bits and none set past
// its length.
func checkRPAPrefixes(prefixes []RPAPrefix) error {
	for _, p := range prefixes {
		if p.Bits == 0 || p.Bits > 16 || (p.Bits < 16 && p.Value>>p.Bits != 0) {
			return fmt.Errorf("%w: %x/%d", ErrRPAPrefix, p.Value, p.Bits)
		}
	}
	return nil
}

// rpaPrefixesMatch reports whether sorted filter values hold one with any of
// the prefixes.
func rpaPrefixesMatch(values []uint16, prefixes []RPAPrefix) bool {
	for _, p := range prefixes {
		if builder.RPAFilterMatch(values, p.Value, p.Bits) {
			return true
		}
	}
	return false
}

// rpaInputsOf returns the inputs of a block whose values have any of the
// prefixes: of the first 30 inputs of each transaction but the coinbase.
func rpaInputsOf(block *wire.MsgBlock, prefixes []RPAPrefix) []RPAInput {
	var inputs []RPAInput
	for i, tx := range block.Transactions {
		if i == 0 {
			continue
		}
		for j, txIn := range tx.TxIn {
			if j == builder.RPAFilterInputs {
				break
			}
			v := builder.RPAInputValue(txIn)
			if rpaPrefixesMatch([]uint16{v}, prefixes) {
				inputs = append(inputs, RPAInput{TxIndex: i, Index: j, Value: v})
			}
		}
	}
	return inputs
}

// agreeRPAFilterHashes returns the filter hashes of the blocks from start to
// stop that the peers give, with the peers that give them.  Where peers
// disagree on a block, the block is downloaded and the peers whose hash for it
// is not the one it gives are dropped.
func agreeRPAFilterHashes(src rpaSource, start, stop int32,
	stopHash chainhash.Hash) ([]chainhash.Hash, []string, error) {

	count := int(stop - start + 1)
	answers := make(map[string][]chainhash.Hash)
	for peer, msg := range src.filterHashes(start, stopHash) {
		if msg.FilterType != wire.GCSFilterInputPrefix || msg.StopHash != stopHash ||
			len(msg.FilterHashes) != count {

			src.bad(peer)
			continue
		}
		hashes := make([]chainhash.Hash, count)
		for i, h := range msg.FilterHashes {
			hashes[i] = *h
		}
		answers[peer] = hashes
	}

	for len(answers) > 0 {
		var peers []string
		for peer := range answers {
			peers = append(peers, peer)
		}
		slices.Sort(peers)
		first := answers[peers[0]]

		// The first block whose filter hash not every peer agrees on.
		at := -1
		for i := 0; i < count && at < 0; i++ {
			for _, peer := range peers[1:] {
				if answers[peer][i] != first[i] {
					at = i
					break
				}
			}
		}
		if at < 0 {
			return first, peers, nil
		}

		hash, err := src.blockHash(start + int32(at))
		if err != nil {
			return nil, nil, err
		}
		block, err := src.block(hash)
		if err != nil {
			return nil, nil, err
		}
		want := builder.GetRPAFilterHash(builder.BuildRPAFilter(block.MsgBlock()))
		for _, peer := range peers {
			if answers[peer][at] != want {
				log.Warnf("Peer %v serves a wrong input prefix filter "+
					"hash for block %v", peer, hash)
				src.bad(peer)
				delete(answers, peer)
			}
		}
	}
	return nil, nil, ErrNoRPAFilterPeers
}

// fetchRPAFilters fetches the filters of the blocks from start to stop from
// one of the peers, in random order, that gives them all with the agreed
// hashes.  A peer that gives others is dropped.
func fetchRPAFilters(src rpaSource, peers []string, start int32,
	stopHash chainhash.Hash, hashes []chainhash.Hash) ([][]byte, error) {

	order := slices.Clone(peers)
	rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	for _, peer := range order {
		msgs := src.filters(peer, start, stopHash, len(hashes))
		if len(msgs) != len(hashes) {
			continue
		}

		// The filters can arrive in another order than the blocks', so
		// each is taken by its block's hash.
		byBlock := make(map[chainhash.Hash]*wire.MsgCFilter, len(msgs))
		for _, msg := range msgs {
			byBlock[msg.BlockHash] = msg
		}
		filters := make([][]byte, len(hashes))
		ok := true
		for i := range hashes {
			blockHash, err := src.blockHash(start + int32(i))
			if err != nil {
				return nil, err
			}
			msg := byBlock[blockHash]
			if msg == nil || msg.FilterType != wire.GCSFilterInputPrefix ||
				builder.GetRPAFilterHash(msg.Data) != hashes[i] {

				ok = false
				break
			}
			filters[i] = msg.Data
		}
		if !ok {
			log.Warnf("Peer %v serves an input prefix filter that is not "+
				"the one whose hash the peers agree on", peer)
			src.bad(peer)
			continue
		}
		return filters, nil
	}
	return nil, ErrNoRPAFilterPeers
}

// searchRPAPrefixes searches the blocks from start to end, both included, for
// inputs with any of the prefixes, and hands each block that has some to
// found, in order, until found returns false.  It returns the last height
// searched.
func searchRPAPrefixes(src rpaSource, prefixes []RPAPrefix, start, end int32,
	found func(RPAMatch) bool) (int32, error) {

	if err := checkRPAPrefixes(prefixes); err != nil {
		return start - 1, err
	}
	for from := start; from <= end; from += rpaFilterBatch {
		to := min(from+rpaFilterBatch-1, end)
		stopHash, err := src.blockHash(to)
		if err != nil {
			return from - 1, err
		}
		hashes, peers, err := agreeRPAFilterHashes(src, from, to, stopHash)
		if err != nil {
			return from - 1, err
		}
		filters, err := fetchRPAFilters(src, peers, from, stopHash, hashes)
		if err != nil {
			return from - 1, err
		}

		for i, filter := range filters {
			height := from + int32(i)
			values, err := builder.DecodeRPAFilter(filter)

			// A filter that doesn't decode is checked against its
			// block like one that matches.
			if err == nil && !rpaPrefixesMatch(values, prefixes) {
				continue
			}
			hash, err := src.blockHash(height)
			if err != nil {
				return height - 1, err
			}
			block, err := src.block(hash)
			if err != nil {
				return height - 1, err
			}
			if !bytes.Equal(builder.BuildRPAFilter(block.MsgBlock()), filter) {
				for _, peer := range peers {
					src.bad(peer)
				}
				return height - 1, fmt.Errorf("%w: block %v", ErrRPAFilterMismatch, hash)
			}
			inputs := rpaInputsOf(block.MsgBlock(), prefixes)
			if len(inputs) == 0 {
				continue
			}
			if !found(RPAMatch{Height: height, Block: block, Inputs: inputs}) {
				return height, nil
			}
		}
	}
	return end, nil
}

// SearchRPAPrefixes searches the blocks of our chain from start to end, both
// included, for inputs whose values have any of the prefixes, as a wallet on
// a reusable payment address looks for its payments, and hands each block
// that has some to found, in order, until found returns false.  It returns the
// last height searched.  It reads the input prefix filters (filter type 2) of
// the peers that serve them and downloads only the blocks whose filters
// match, so no request names a prefix; a peer shown to serve a wrong filter
// is banned.  The blocks downloaded are what a peer can learn from: see
// gcash/bchd's filter type 2 on making them unlinkable.
func (s *ChainService) SearchRPAPrefixes(prefixes []RPAPrefix, start, end int32,
	found func(RPAMatch) bool) (int32, error) {

	return searchRPAPrefixes(&chainRPASource{s: s}, prefixes, start, end, found)
}

// chainRPASource reads blocks and filters from a ChainService's peers.
type chainRPASource struct {
	s *ChainService
}

func (c *chainRPASource) blockHash(height int32) (chainhash.Hash, error) {
	header, err := c.s.BlockHeaders.FetchHeaderByHeight(uint32(height))
	if err != nil {
		return chainhash.Hash{}, err
	}
	return header.BlockHash(), nil
}

func (c *chainRPASource) filterHashes(start int32,
	stop chainhash.Hash) map[string]*wire.MsgCFHeaders {

	var (
		mtx     sync.Mutex
		wg      sync.WaitGroup
		answers = make(map[string]*wire.MsgCFHeaders)
	)
	for _, sp := range c.s.Peers() {
		if !sp.Connected() {
			continue
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			msg := c.s.askPeer(peer,
				wire.NewMsgGetCFHeaders(wire.GCSFilterInputPrefix, uint32(start), &stop),
				func(msg wire.Message) bool {
					headers, ok := msg.(*wire.MsgCFHeaders)
					return ok && headers.FilterType == wire.GCSFilterInputPrefix &&
						headers.StopHash == stop
				}, rpaFilterTimeout, c.s.quit,
			)
			if msg == nil {
				return
			}
			mtx.Lock()
			answers[peer] = msg.(*wire.MsgCFHeaders)
			mtx.Unlock()
		}(sp.Addr())
	}
	wg.Wait()
	return answers
}

func (c *chainRPASource) filters(peer string, start int32, stop chainhash.Hash,
	count int) []*wire.MsgCFilter {

	// A peer's messages are handed to us each on its own goroutine, so
	// they can arrive out of order; one filter for each block is taken.
	var msgs []*wire.MsgCFilter
	seen := make(map[chainhash.Hash]bool, count)
	last := c.s.askPeer(peer,
		wire.NewMsgGetCFilters(wire.GCSFilterInputPrefix, uint32(start), &stop),
		func(msg wire.Message) bool {
			filter, ok := msg.(*wire.MsgCFilter)
			if !ok || filter.FilterType != wire.GCSFilterInputPrefix || seen[filter.BlockHash] {
				return false
			}
			seen[filter.BlockHash] = true
			msgs = append(msgs, filter)
			return len(msgs) == count
		}, rpaFilterTimeout, c.s.quit,
	)
	if last == nil {
		return nil
	}
	return msgs
}

func (c *chainRPASource) block(hash chainhash.Hash) (*bchutil.Block, error) {
	return c.s.GetBlock(hash)
}

func (c *chainRPASource) bad(peer string) {
	if err := c.s.BanPeer(peer, banman.InvalidFilterHeader); err != nil {
		log.Errorf("Unable to ban peer %v: %v", peer, err)
	}
	if sp := c.s.PeerByAddr(peer); sp != nil {
		sp.Disconnect()
	}
}

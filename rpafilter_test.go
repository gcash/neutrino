package neutrino

import (
	"errors"
	"slices"
	"testing"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs/builder"
)

// rpaTestPeer serves the input prefix filters of a chain, as a node does, or
// changed by edit.
type rpaTestPeer struct {
	silent bool

	// edit, when set, changes the filter a peer serves for a height.
	edit func(height int32, filter []byte) []byte
}

// rpaTestChain is a chain of blocks with the peers that serve its filters.
type rpaTestChain struct {
	blocks []*bchutil.Block
	peers  map[string]*rpaTestPeer
	banned []string

	// downloads counts the blocks downloaded, by height.
	downloads map[int32]int
}

func (c *rpaTestChain) blockHash(height int32) (chainhash.Hash, error) {
	if height < 0 || int(height) >= len(c.blocks) {
		return chainhash.Hash{}, errors.New("no such height")
	}
	return *c.blocks[height].Hash(), nil
}

func (c *rpaTestChain) heightOf(hash chainhash.Hash) int32 {
	for i, b := range c.blocks {
		if *b.Hash() == hash {
			return int32(i)
		}
	}
	return -1
}

func (c *rpaTestChain) served(peer string, height int32) []byte {
	filter := builder.BuildRPAFilter(c.blocks[height].MsgBlock())
	if p := c.peers[peer]; p.edit != nil {
		filter = p.edit(height, filter)
	}
	return filter
}

func (c *rpaTestChain) filterHashes(start int32, stop chainhash.Hash) map[string]*wire.MsgCFHeaders {
	answers := make(map[string]*wire.MsgCFHeaders)
	for name, p := range c.peers {
		if p.silent || slices.Contains(c.banned, name) {
			continue
		}
		msg := &wire.MsgCFHeaders{FilterType: wire.GCSFilterInputPrefix, StopHash: stop}
		for h := start; h <= c.heightOf(stop); h++ {
			hash := builder.GetRPAFilterHash(c.served(name, h))
			msg.FilterHashes = append(msg.FilterHashes, &hash)
		}
		answers[name] = msg
	}
	return answers
}

func (c *rpaTestChain) filters(peer string, start int32, stop chainhash.Hash, count int) []*wire.MsgCFilter {
	if c.peers[peer].silent {
		return nil
	}
	var msgs []*wire.MsgCFilter
	for h := start; h <= c.heightOf(stop); h++ {
		msgs = append(msgs, wire.NewMsgCFilter(wire.GCSFilterInputPrefix, c.blocks[h].Hash(), c.served(peer, h)))
	}
	return msgs
}

func (c *rpaTestChain) block(hash chainhash.Hash) (*bchutil.Block, error) {
	h := c.heightOf(hash)
	if h < 0 {
		return nil, errors.New("no such block")
	}
	c.downloads[h]++
	return c.blocks[h], nil
}

func (c *rpaTestChain) bad(peer string) {
	if !slices.Contains(c.banned, peer) {
		c.banned = append(c.banned, peer)
	}
}

// rpaTestTx returns a transaction spending count outpoints of a txid made
// from tag.
func rpaTestTx(tag byte, count int) *wire.MsgTx {
	tx := wire.NewMsgTx(2)
	for i := 0; i < count; i++ {
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{tag, 0x52}, uint32(i)), nil))
	}
	tx.AddTxOut(wire.NewTxOut(1000, []byte{0x51}, wire.TokenData{}))
	return tx
}

// newRPATestChain returns a chain of n blocks: block h holds a coinbase and,
// at every third height, a transaction with h%40+1 inputs, served honestly by
// one peer named "honest".
func newRPATestChain(n int) *rpaTestChain {
	c := &rpaTestChain{
		peers:     map[string]*rpaTestPeer{"honest": {}},
		downloads: make(map[int32]int),
	}
	var prev chainhash.Hash
	for h := 0; h < n; h++ {
		coinbase := wire.NewMsgTx(1)
		coinbase.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{}, wire.MaxPrevOutIndex),
			[]byte{byte(h), byte(h >> 8)}))
		coinbase.AddTxOut(wire.NewTxOut(5000, []byte{0x51}, wire.TokenData{}))
		msg := wire.NewMsgBlock(&wire.BlockHeader{Version: 4, PrevBlock: prev, Nonce: uint32(h)})
		msg.Transactions = []*wire.MsgTx{coinbase}
		if h%3 == 1 {
			msg.Transactions = append(msg.Transactions, rpaTestTx(byte(h), h%40+1))
		}
		block := bchutil.NewBlock(msg)
		c.blocks = append(c.blocks, block)
		prev = *block.Hash()
	}
	return c
}

// rpaValuesAt returns the values of the inputs of the block at a height.
func (c *rpaTestChain) inputsAt(height int32) []uint16 {
	var values []uint16
	for _, tx := range c.blocks[height].MsgBlock().Transactions[1:] {
		for _, txIn := range tx.TxIn {
			values = append(values, builder.RPAInputValue(txIn))
		}
	}
	return values
}

func (c *rpaTestChain) search(t *testing.T, prefixes []RPAPrefix, start, end int32) ([]RPAMatch, int32, error) {
	t.Helper()
	var found []RPAMatch
	reached, err := searchRPAPrefixes(c, prefixes, start, end, func(m RPAMatch) bool {
		found = append(found, m)
		return true
	})
	return found, reached, err
}

// TestSearchRPAPrefixes searches a chain longer than one batch for the full
// value of one input and for a short prefix of another, and checks that it
// finds exactly the blocks with those inputs, downloads only the blocks whose
// filters match, and leaves out the 31st input of a transaction.
func TestSearchRPAPrefixes(t *testing.T) {
	defer func(batch int32) { rpaFilterBatch = batch }(rpaFilterBatch)
	rpaFilterBatch = 10

	c := newRPATestChain(35)
	target := c.inputsAt(4)[2]
	found, reached, err := c.search(t, []RPAPrefix{{Value: target, Bits: 16}}, 0, 34)
	if err != nil || reached != 34 {
		t.Fatalf("reached %d, %v", reached, err)
	}
	if len(found) == 0 || found[0].Height != 4 {
		t.Fatalf("found %+v, want block 4 first", found)
	}
	for _, m := range found {
		for _, in := range m.Inputs {
			if in.Value != target || in.Index >= 30 || in.TxIndex == 0 {
				t.Errorf("block %d: input %+v", m.Height, in)
			}
		}
	}
	for h, n := range c.downloads {
		values, _ := builder.DecodeRPAFilter(builder.BuildRPAFilter(c.blocks[h].MsgBlock()))
		if n != 1 || !builder.RPAFilterMatch(values, target, 16) {
			t.Errorf("block %d downloaded %d times, its filter matching %v",
				h, n, builder.RPAFilterMatch(values, target, 16))
		}
	}

	// A 4-bit prefix of an input of block 31 finds block 31 among others.
	short := RPAPrefix{Value: c.inputsAt(31)[0] >> 12, Bits: 4}
	found, _, err = c.search(t, []RPAPrefix{short}, 20, 34)
	if err != nil || !slices.ContainsFunc(found, func(m RPAMatch) bool { return m.Height == 31 }) {
		t.Fatalf("4-bit prefix: found %+v, %v", found, err)
	}

	// Block 31's transaction has 32 inputs: the value of its 31st is in no
	// filter unless another input has it.
	inputs := c.inputsAt(31)
	if len(inputs) != 32 {
		t.Fatalf("block 31 has %d inputs", len(inputs))
	}
	for _, v := range inputs[30:] {
		if slices.Contains(inputs[:30], v) {
			continue
		}
		found, _, err := c.search(t, []RPAPrefix{{Value: v, Bits: 16}}, 31, 31)
		if err != nil || len(found) != 0 {
			t.Errorf("the value %04x of an input past the 30th found %+v, %v", v, found, err)
		}
	}

	// found returning false stops the search at that block.
	reached, err = searchRPAPrefixes(c, []RPAPrefix{{Value: target, Bits: 16}}, 0, 34,
		func(RPAMatch) bool { return false })
	if err != nil || reached != 4 {
		t.Errorf("stopped at %d, %v; want 4", reached, err)
	}

	// Prefixes that are not ones are refused.
	for _, p := range []RPAPrefix{{0, 0}, {0, 17}, {0x1ff, 8}} {
		if _, _, err := c.search(t, []RPAPrefix{p}, 0, 34); !errors.Is(err, ErrRPAPrefix) {
			t.Errorf("prefix %x/%d: %v", p.Value, p.Bits, err)
		}
	}
}

// TestSearchRPAPrefixesDisagreement pits an honest peer against one that
// leaves the target's value out of a filter, which would hide the payment:
// the disagreement on that block's filter hash is settled by the block, the
// liar is dropped, and the payment is found.
func TestSearchRPAPrefixesDisagreement(t *testing.T) {
	c := newRPATestChain(20)
	target := c.inputsAt(7)[0]
	c.peers["liar"] = &rpaTestPeer{edit: func(height int32, filter []byte) []byte {
		if height != 7 {
			return filter
		}
		values, _ := builder.DecodeRPAFilter(filter)
		values = slices.DeleteFunc(values, func(v uint16) bool { return v == target })
		return builder.EncodeRPAFilter(values)
	}}
	found, _, err := c.search(t, []RPAPrefix{{Value: target, Bits: 16}}, 0, 19)
	if err != nil || len(found) == 0 || found[0].Height != 7 {
		t.Fatalf("found %+v, %v", found, err)
	}
	if !slices.Equal(c.banned, []string{"liar"}) {
		t.Errorf("banned %v, want the liar", c.banned)
	}
	if c.downloads[7] != 2 {
		t.Errorf("block 7 downloaded %d times, want once to settle and once to search", c.downloads[7])
	}
}

// TestSearchRPAPrefixesWrongFilters checks the peers that agree on wrong
// filters: one that matches a block it shouldn't is shown wrong by the block,
// one whose filter bytes don't have the agreed hash is dropped for another,
// and one that serves no hashes leaves the search without peers.
func TestSearchRPAPrefixesWrongFilters(t *testing.T) {
	c := newRPATestChain(20)
	const fake = 0xabcd
	c.peers["honest"].edit = func(height int32, filter []byte) []byte {
		if height != 5 {
			return filter
		}
		values, _ := builder.DecodeRPAFilter(filter)
		values = append(values, fake)
		slices.Sort(values)
		return builder.EncodeRPAFilter(slices.Compact(values))
	}
	_, reached, err := c.search(t, []RPAPrefix{{Value: fake, Bits: 16}}, 0, 19)
	if !errors.Is(err, ErrRPAFilterMismatch) || reached != 4 || !slices.Equal(c.banned, []string{"honest"}) {
		t.Fatalf("a filter with a value its block lacks: reached %d, banned %v, %v", reached, c.banned, err)
	}

	// Filters whose bytes don't have the hashes the peer gave: the peer
	// is dropped and the filters are taken from another. The peer to ask
	// is picked at random, so the search runs until the liar was asked.
	checked := false
	for run := 0; run < 64 && !checked; run++ {
		c = newRPATestChain(20)
		c.peers["switcher"] = &rpaTestPeer{}
		target := c.inputsAt(10)[0]
		switcher := &rpaSwitchSource{rpaTestChain: c, peer: "switcher"}
		var found []RPAMatch
		_, err := searchRPAPrefixes(switcher, []RPAPrefix{{Value: target, Bits: 16}}, 0, 19,
			func(m RPAMatch) bool { found = append(found, m); return true })
		if err != nil || len(found) == 0 || found[0].Height != 10 {
			t.Fatalf("found %+v, %v", found, err)
		}
		if switcher.asked == 0 {
			continue
		}
		checked = true
		if !slices.Equal(c.banned, []string{"switcher"}) {
			t.Errorf("banned %v, want the peer whose filters don't have its hashes", c.banned)
		}
	}
	if !checked {
		t.Error("the peer whose filters don't have its hashes was never asked")
	}

	// No peer answers.
	c = newRPATestChain(5)
	c.peers["honest"].silent = true
	if _, _, err := c.search(t, []RPAPrefix{{Value: 1, Bits: 16}}, 0, 4); !errors.Is(err, ErrNoRPAFilterPeers) {
		t.Errorf("without peers: %v", err)
	}
}

// rpaSwitchSource serves, for one peer, filters other than those whose hashes
// it gave.
type rpaSwitchSource struct {
	*rpaTestChain
	peer  string
	asked int
}

func (s *rpaSwitchSource) filters(peer string, start int32, stop chainhash.Hash, count int) []*wire.MsgCFilter {
	msgs := s.rpaTestChain.filters(peer, start, stop, count)
	if peer == s.peer {
		s.asked++
		msgs[len(msgs)-1].Data = []byte{0x00}
	}
	return msgs
}

// rpaShuffleSource serves each run's filters in reverse order, as a peer's
// messages can arrive out of order.
type rpaShuffleSource struct {
	*rpaTestChain
}

func (s *rpaShuffleSource) filters(peer string, start int32, stop chainhash.Hash, count int) []*wire.MsgCFilter {
	msgs := s.rpaTestChain.filters(peer, start, stop, count)
	slices.Reverse(msgs)
	return msgs
}

// TestSearchRPAPrefixesOutOfOrder checks that filters arriving out of order
// are each taken for their own block.
func TestSearchRPAPrefixesOutOfOrder(t *testing.T) {
	c := newRPATestChain(20)
	target := c.inputsAt(13)[0]
	var found []RPAMatch
	_, err := searchRPAPrefixes(&rpaShuffleSource{c}, []RPAPrefix{{Value: target, Bits: 16}}, 0, 19,
		func(m RPAMatch) bool { found = append(found, m); return true })
	if err != nil || len(found) == 0 || found[0].Height != 13 || len(c.banned) != 0 {
		t.Fatalf("found %+v, banned %v, %v", found, c.banned, err)
	}
}

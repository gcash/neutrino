package neutrino

import (
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// spendTestChain is a chain of blocks with real filters, by height from 1.
type spendTestChain struct {
	blocks  map[int32]*wire.MsgBlock
	filters map[chainhash.Hash]*gcs.Filter
	hashes  map[int32]chainhash.Hash
	fetched []int32
	batches []optimisticBatchType
	failAt  int32
	stopAt  int32
	stop    chan struct{}
}

// spendTestTx returns a transaction that spends the outpoints, with a unique
// output.
func spendTestTx(id int64, spends ...wire.OutPoint) *wire.MsgTx {
	tx := wire.NewMsgTx(2)
	for _, outpoint := range spends {
		tx.AddTxIn(wire.NewTxIn(&outpoint, nil))
	}
	tx.AddTxOut(wire.NewTxOut(id, []byte{0x51}, wire.TokenData{}))
	return tx
}

// newSpendTestChain returns a chain of height blocks, with the transactions of
// txs in the blocks they are keyed by.
func newSpendTestChain(t *testing.T, height int32, txs map[int32][]*wire.MsgTx) *spendTestChain {
	t.Helper()
	c := &spendTestChain{
		blocks:  make(map[int32]*wire.MsgBlock),
		filters: make(map[chainhash.Hash]*gcs.Filter),
		hashes:  make(map[int32]chainhash.Hash),
	}
	for h := int32(1); h <= height; h++ {
		coinbase := wire.NewMsgTx(1)
		coinbase.AddTxIn(wire.NewTxIn(&wire.OutPoint{Index: ^uint32(0)}, []byte{byte(h), byte(h >> 8)}))
		coinbase.AddTxOut(wire.NewTxOut(1, []byte{0x51}, wire.TokenData{}))
		block := &wire.MsgBlock{
			Header:       wire.BlockHeader{Nonce: uint32(h)},
			Transactions: append([]*wire.MsgTx{coinbase}, txs[h]...),
		}
		filter, err := builder.BuildBasicFilter(block)
		if err != nil {
			t.Fatal(err)
		}
		hash := block.BlockHash()
		c.blocks[h], c.filters[hash], c.hashes[h] = block, filter, hash
	}
	return c
}

func (c *spendTestChain) GetBlockHash(height int64) (*chainhash.Hash, error) {
	if int32(height) == c.stopAt && c.stop != nil {
		close(c.stop)
		c.stop = nil
	}
	hash, ok := c.hashes[int32(height)]
	if !ok {
		return nil, fmt.Errorf("no block at %d", height)
	}
	return &hash, nil
}

func (c *spendTestChain) GetCFilter(hash chainhash.Hash, _ wire.FilterType,
	options ...QueryOption) (*gcs.Filter, error) {

	var qo queryOptions
	for _, option := range options {
		option(&qo)
	}
	c.batches = append(c.batches, qo.optimisticBatch)
	for h, blockHash := range c.hashes {
		if blockHash == hash && h == c.failAt {
			return nil, errors.New("no peer sent the filter")
		}
	}
	return c.filters[hash], nil
}

func (c *spendTestChain) GetBlock(hash chainhash.Hash, _ ...QueryOption) (*bchutil.Block, error) {
	for h, blockHash := range c.hashes {
		if blockHash == hash {
			c.fetched = append(c.fetched, h)
			return bchutil.NewBlock(c.blocks[h]), nil
		}
	}
	return nil, errors.New("no such block")
}

// search runs a search with no way to stop it.
func (c *spendTestChain) search(outpoints []wire.OutPoint, from, to int32,
	found func(FoundSpend) []wire.OutPoint) (int32, error) {

	return searchSpends(c, outpoints, from, to, found, nil, nil, nil)
}

// outPointOf returns output 0 of tx.
func outPointOf(tx *wire.MsgTx) wire.OutPoint {
	return wire.OutPoint{Hash: tx.TxHash()}
}

// TestSearchSpendsBothWays ensures a search finds the spends of several
// outpoints in either direction, in that order, and stops once they're found.
func TestSearchSpendsBothWays(t *testing.T) {
	a, b := wire.OutPoint{Hash: chainhash.Hash{1}}, wire.OutPoint{Hash: chainhash.Hash{2}}
	spendA, spendB := spendTestTx(1, a), spendTestTx(2, wire.OutPoint{Hash: chainhash.Hash{9}}, b)
	c := newSpendTestChain(t, 20, map[int32][]*wire.MsgTx{5: {spendA}, 12: {spendB}})

	for _, test := range []struct {
		from, to, reached int32
		want              []int32
	}{
		{1, 20, 12, []int32{5, 12}},
		{20, 1, 5, []int32{12, 5}},
	} {
		var got []int32
		var spends []FoundSpend
		reached, err := c.search([]wire.OutPoint{a, b}, test.from, test.to,
			func(f FoundSpend) []wire.OutPoint {
				got = append(got, f.Height)
				spends = append(spends, f)
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) || reached != test.reached {
			t.Fatalf("from %d: found at %v, reached %d", test.from, got, reached)
		}
		for _, spend := range spends {
			if spend.BlockHash != c.hashes[spend.Height] {
				t.Fatalf("block %v at %d", spend.BlockHash, spend.Height)
			}
			if spend.SpendingTx.TxHash() == spendB.TxHash() &&
				(spend.OutPoint != b || spend.SpendingInputIndex != 1) {

				t.Fatalf("spend of b: %+v", spend)
			}
		}
	}
}

// TestSearchSpendsBatches ensures each direction asks for the filters it is
// about to read in one go.
func TestSearchSpendsBatches(t *testing.T) {
	c := newSpendTestChain(t, 3, nil)
	outpoint := []wire.OutPoint{{Hash: chainhash.Hash{1}}}
	if _, err := c.search(outpoint, 1, 3, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.search(outpoint, 3, 1, nil); err != nil {
		t.Fatal(err)
	}
	want := []optimisticBatchType{forwardBatch, forwardBatch, forwardBatch,
		reverseBatch, reverseBatch, reverseBatch}
	if !reflect.DeepEqual(c.batches, want) {
		t.Fatalf("batches %v", c.batches)
	}
}

// TestSearchSpendsLimits ensures a search reads exactly its range, both ends
// included, and that an outpoint spent outside it isn't found.
func TestSearchSpendsLimits(t *testing.T) {
	a := wire.OutPoint{Hash: chainhash.Hash{1}}
	c := newSpendTestChain(t, 20, map[int32][]*wire.MsgTx{10: {spendTestTx(1, a)}})

	for _, test := range []struct {
		from, to int32
		found    bool
		reached  int32
	}{
		{10, 20, true, 10},
		{1, 10, true, 10},
		{20, 10, true, 10},
		{10, 1, true, 10},
		{11, 20, false, 20},
		{1, 9, false, 9},
		{20, 11, false, 11},
		{9, 1, false, 1},
		{10, 10, true, 10},
		{11, 11, false, 11},
	} {
		found := false
		reached, err := c.search([]wire.OutPoint{a}, test.from, test.to,
			func(FoundSpend) []wire.OutPoint {
				found = true
				return nil
			})
		if err != nil || found != test.found || reached != test.reached {
			t.Fatalf("%+v: found %v, reached %d, %v", test, found, reached, err)
		}
	}
}

// TestSearchSpendsNothing ensures a search for nothing reads nothing.
func TestSearchSpendsNothing(t *testing.T) {
	c := newSpendTestChain(t, 5, nil)
	reached, err := c.search(nil, 2, 5, nil)
	if err != nil || reached != 1 || len(c.batches) != 0 {
		t.Fatalf("reached %d, %d filters, %v", reached, len(c.batches), err)
	}
}

// TestSearchSpendsFollows ensures one pass up follows a chain of spends, also
// through several links in one block, whichever order the block lists them in,
// as a block is sorted by txid, and that the chain then stops at an unspent link.
func TestSearchSpendsFollows(t *testing.T) {
	start := wire.OutPoint{Hash: chainhash.Hash{1}}
	first := spendTestTx(1, start)
	second := spendTestTx(2, outPointOf(first))
	third := spendTestTx(3, outPointOf(second))
	fourth := spendTestTx(4, outPointOf(third))
	c := newSpendTestChain(t, 20, map[int32][]*wire.MsgTx{
		3: {first},
		8: {fourth, third, second},
	})
	var got []chainhash.Hash
	reached, err := c.search([]wire.OutPoint{start}, 1, 20,
		func(f FoundSpend) []wire.OutPoint {
			got = append(got, f.SpendingTx.TxHash())
			return []wire.OutPoint{outPointOf(f.SpendingTx)}
		})
	if err != nil {
		t.Fatal(err)
	}
	want := []chainhash.Hash{first.TxHash(), second.TxHash(), third.TxHash(), fourth.TxHash()}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("followed %v, want %v", got, want)
	}
	if reached != 20 {
		t.Fatalf("reached %d, want the end, as the last link stays watched", reached)
	}
	if !reflect.DeepEqual(c.fetched, []int32{3, 8}) {
		t.Fatalf("fetched blocks %v", c.fetched)
	}
}

// TestSearchSpendsBlockReportsOnce ensures that outpoints spent in one block
// are each reported once, in the order of the block, also when found asks to
// watch one that is watched already.
func TestSearchSpendsBlockReportsOnce(t *testing.T) {
	a, b := wire.OutPoint{Hash: chainhash.Hash{1}}, wire.OutPoint{Hash: chainhash.Hash{2}}
	spendA, spendB := spendTestTx(1, a), spendTestTx(2, b)
	after := spendTestTx(3, outPointOf(spendA))
	c := newSpendTestChain(t, 5, map[int32][]*wire.MsgTx{3: {spendB, spendA, after}})

	var got []wire.OutPoint
	_, err := c.search([]wire.OutPoint{a, b}, 1, 5, func(f FoundSpend) []wire.OutPoint {
		got = append(got, f.OutPoint)
		if f.OutPoint == a {
			return []wire.OutPoint{b, outPointOf(spendA)}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []wire.OutPoint{b, a, outPointOf(spendA)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reported %v, want %v", got, want)
	}
}

// TestSearchSpendsDownDoesntFollow ensures a search down, where a spender's
// outputs could only be spent at heights already read, ignores what found
// returns.
func TestSearchSpendsDownDoesntFollow(t *testing.T) {
	start := wire.OutPoint{Hash: chainhash.Hash{1}}
	first := spendTestTx(1, start)
	c := newSpendTestChain(t, 5, map[int32][]*wire.MsgTx{3: {first}})
	reached, err := c.search([]wire.OutPoint{start}, 5, 1,
		func(f FoundSpend) []wire.OutPoint {
			return []wire.OutPoint{outPointOf(f.SpendingTx)}
		})
	if err != nil || reached != 3 {
		t.Fatalf("reached %d, %v", reached, err)
	}
}

// TestSearchSpendsFalsePositive ensures a block whose filter matches but that
// doesn't hold the spend is read and passed over.
func TestSearchSpendsFalsePositive(t *testing.T) {
	a := wire.OutPoint{Hash: chainhash.Hash{1}}
	spend := spendTestTx(1, a)
	c := newSpendTestChain(t, 10, map[int32][]*wire.MsgTx{7: {spend}})

	// Block 3's filter now claims the outpoint, as a false positive would.
	key := c.hashes[3]
	falseFilter, err := gcs.BuildGCSFilter(builder.DefaultP, builder.DefaultM,
		builder.DeriveKey(&key), [][]byte{spendEntry(t, a)})
	if err != nil {
		t.Fatal(err)
	}
	c.filters[key] = falseFilter

	var got []int32
	reached, err := c.search([]wire.OutPoint{a}, 1, 10, func(f FoundSpend) []wire.OutPoint {
		got = append(got, f.Height)
		return nil
	})
	if err != nil || reached != 7 || !reflect.DeepEqual(got, []int32{7}) {
		t.Fatalf("found at %v, reached %d, %v", got, reached, err)
	}
	if !reflect.DeepEqual(c.fetched, []int32{3, 7}) {
		t.Fatalf("fetched blocks %v", c.fetched)
	}
}

// spendEntry returns the filter entry of an outpoint.
func spendEntry(t *testing.T, outpoint wire.OutPoint) []byte {
	t.Helper()
	entries, err := spendFilterEntries(map[wire.OutPoint]struct{}{outpoint: {}})
	if err != nil {
		t.Fatal(err)
	}
	return entries[0]
}

// TestSearchSpendsResume ensures a search that stopped, for a filter no peer
// sent or because it was told to, says how far it got, and that resuming after
// that height finds what is left.
func TestSearchSpendsResume(t *testing.T) {
	a := wire.OutPoint{Hash: chainhash.Hash{1}}

	t.Run("failed filter", func(t *testing.T) {
		c := newSpendTestChain(t, 10, map[int32][]*wire.MsgTx{8: {spendTestTx(1, a)}})
		c.failAt = 5
		reached, err := c.search([]wire.OutPoint{a}, 1, 10, nil)
		if err == nil || reached != 4 {
			t.Fatalf("reached %d, %v", reached, err)
		}
		c.failAt = 0
		var found int32
		reached, err = c.search([]wire.OutPoint{a}, reached+1, 10,
			func(f FoundSpend) []wire.OutPoint {
				found = f.Height
				return nil
			})
		if err != nil || found != 8 || reached != 8 {
			t.Fatalf("found at %d, reached %d, %v", found, reached, err)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		c := newSpendTestChain(t, 10, nil)
		quit := make(chan struct{})
		var done atomic.Int32
		c.stop, c.stopAt = quit, 6
		reached, err := searchSpends(c, []wire.OutPoint{a}, 10, 1, nil,
			func(height int32) { done.Store(height) }, quit, nil)
		if !errors.Is(err, ErrSpendSearchCancelled) || reached != 6 || done.Load() != 6 {
			t.Fatalf("reached %d, progress %d, %v", reached, done.Load(), err)
		}
	})

	t.Run("nothing finished", func(t *testing.T) {
		c := newSpendTestChain(t, 10, nil)
		quit := make(chan struct{})
		close(quit)
		reached, err := searchSpends(c, []wire.OutPoint{a}, 4, 10, nil, nil, quit, nil)
		if !errors.Is(err, ErrSpendSearchCancelled) || reached != 3 {
			t.Fatalf("reached %d, %v", reached, err)
		}
		reached, err = searchSpends(c, []wire.OutPoint{a}, 4, 1, nil, nil, quit, nil)
		if !errors.Is(err, ErrSpendSearchCancelled) || reached != 5 {
			t.Fatalf("reached %d, %v", reached, err)
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		c := newSpendTestChain(t, 3, nil)
		shutdown := make(chan struct{})
		close(shutdown)
		_, err := searchSpends(c, []wire.OutPoint{a}, 1, 3, nil, nil, nil, shutdown)
		if !errors.Is(err, ErrSpendSearchCancelled) {
			t.Fatal(err)
		}
	})
}

// TestSearchSpendsMissingBlock ensures a block past the chain ends the search
// with an error rather than passing over it.
func TestSearchSpendsMissingBlock(t *testing.T) {
	c := newSpendTestChain(t, 5, nil)
	reached, err := c.search([]wire.OutPoint{{Hash: chainhash.Hash{1}}}, 3, 9, nil)
	if err == nil || reached != 5 {
		t.Fatalf("reached %d, %v", reached, err)
	}
}

// quitDuringQuery is a chain whose filter query ends when its quit closes, as a
// peer that holds the query up would have it do.
type quitDuringQuery struct {
	*spendTestChain
	quit chan struct{}
}

func (c *quitDuringQuery) GetCFilter(chainhash.Hash, wire.FilterType,
	...QueryOption) (*gcs.Filter, error) {

	close(c.quit)
	return nil, ErrFilterFetchFailed
}

// TestSearchSpendsQuitDuringQuery ensures a query that a closed quit ended
// cancels the search, rather than reading as a failure.
func TestSearchSpendsQuitDuringQuery(t *testing.T) {
	c := &quitDuringQuery{spendTestChain: newSpendTestChain(t, 5, nil), quit: make(chan struct{})}
	outpoint := wire.OutPoint{Hash: chainhash.Hash{1}}
	_, err := searchSpends(c, []wire.OutPoint{outpoint}, 1, 3, nil, nil, c.quit, nil)
	if !errors.Is(err, ErrSpendSearchCancelled) {
		t.Fatalf("got %v", err)
	}
}

// TestQueryQuitOption ensures the option reaches the query.
func TestQueryQuitOption(t *testing.T) {
	quit := make(chan struct{})
	qo := defaultQueryOptions()
	qo.applyQueryOptions(QueryQuit(quit))
	if qo.quit == nil {
		t.Fatal("no quit")
	}
}

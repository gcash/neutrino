package neutrino

import (
	"errors"
	"slices"
	"testing"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
)

// categoryTestTx returns a transaction whose outputs carry tokens of the
// passed categories, one output each, in order.
func categoryTestTx(categories ...[32]byte) *wire.MsgTx {
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(wire.NewTxIn(&wire.OutPoint{}, nil))
	script := disputeScript(0xa1)
	for _, category := range categories {
		tx.AddTxOut(wire.NewTxOut(1000, script, wire.TokenData{
			CategoryID: category, Amount: 5, BitField: wire.HAS_AMOUNT,
		}))
	}
	return tx
}

// TestSearchCategoryOutputs finds the transactions that create outputs of
// watched token categories on script filters, in block order and then within
// a block, up and down.  It checks that a search stops when found returns
// false, that a category no block has fetches nothing, and that a transaction
// is reported once however many of its outputs carry the category.
func TestSearchCategoryOutputs(t *testing.T) {
	catA, catB, catC := [32]byte{0xaa}, [32]byte{0xbb}, [32]byte{0xcc}
	t1 := categoryTestTx(catA)
	t2 := categoryTestTx(catA)
	t3 := categoryTestTx(catB)
	c := newScriptSearchChain(t, 10, map[int32][]*wire.MsgTx{
		3: {t1},
		6: {t2, t3},
		// No tokens, so a search for a category does not fetch it.
		8: {scriptTestTx(disputeScript(0x11))},
	}, nil)

	var found []FoundOutput
	reached, err := searchCategoryOutputs(c, [][32]byte{catA}, 1, 10,
		func(output FoundOutput) bool {
			found = append(found, output)
			return true
		}, nil, nil, nil)
	if err != nil || reached != 10 || len(found) != 2 ||
		found[0].Tx != t1 || found[0].Height != 3 ||
		found[0].BlockHash != c.hashes[3] ||
		found[1].Tx != t2 || found[1].Height != 6 ||
		found[1].BlockHash != c.hashes[6] {

		t.Fatalf("category A: found %+v, reached %d, %v", found, reached, err)
	}
	if !slices.Equal(c.fetched, []int32{3, 6}) {
		t.Errorf("category A: fetched blocks %v", c.fetched)
	}

	found, c.fetched = nil, nil
	reached, err = searchCategoryOutputs(c, [][32]byte{catA, catB}, 1, 10,
		func(output FoundOutput) bool {
			found = append(found, output)
			return true
		}, nil, nil, nil)
	if err != nil || reached != 10 || len(found) != 3 ||
		found[0].Tx != t1 || found[0].Height != 3 ||
		found[1].Tx != t2 || found[1].Height != 6 ||
		found[2].Tx != t3 || found[2].Height != 6 {

		t.Fatalf("categories A and B: found %+v, reached %d, %v", found, reached, err)
	}

	found, c.fetched = nil, nil
	reached, err = searchCategoryOutputs(c, [][32]byte{catA}, 10, 1,
		func(output FoundOutput) bool {
			found = append(found, output)
			return true
		}, nil, nil, nil)
	if err != nil || reached != 1 || len(found) != 2 ||
		found[0].Tx != t2 || found[0].Height != 6 ||
		found[1].Tx != t1 || found[1].Height != 3 {

		t.Fatalf("category A, down: found %+v, reached %d, %v", found, reached, err)
	}
	if !slices.Equal(c.fetched, []int32{6, 3}) {
		t.Errorf("category A, down: fetched blocks %v", c.fetched)
	}

	// found returns false for T1, the first transaction of category A.
	calls := 0
	c.fetched = nil
	reached, err = searchCategoryOutputs(c, [][32]byte{catA}, 1, 10,
		func(output FoundOutput) bool {
			calls++
			if output.Tx != t1 || output.Height != 3 {
				t.Errorf("stopped on tx %v at %d", output.Tx.TxHash(), output.Height)
			}
			return false
		}, nil, nil, nil)
	if err != nil || reached != 3 || calls != 1 {
		t.Fatalf("stop at T1: reached %d after %d calls, %v", reached, calls, err)
	}

	c.fetched = nil
	reached, err = searchCategoryOutputs(c, [][32]byte{catC}, 1, 10,
		func(FoundOutput) bool {
			t.Error("found an output of a category no block has")
			return true
		}, nil, nil, nil)
	if err != nil || reached != 10 || len(c.fetched) != 0 {
		t.Errorf("category C: fetched blocks %v, reached %d, %v", c.fetched, reached, err)
	}

	// A second chain, where one transaction creates two outputs of category A.
	both := categoryTestTx(catA, catA)
	c2 := newScriptSearchChain(t, 4, map[int32][]*wire.MsgTx{2: {both}}, nil)
	found = nil
	reached, err = searchCategoryOutputs(c2, [][32]byte{catA}, 1, 4,
		func(output FoundOutput) bool {
			found = append(found, output)
			return true
		}, nil, nil, nil)
	if err != nil || reached != 4 || len(found) != 1 ||
		found[0].Tx != both || found[0].Height != 2 || len(both.TxOut) != 2 {

		t.Fatalf("two outputs: found %+v, reached %d, %v", found, reached, err)
	}
}

// TestSearchCategoryOutputsNeedsScriptFilters checks that a category search
// on regular filters is refused with ErrCategoriesNeedScriptFilters and
// returns the height before the start, in the direction of the search, and
// that found is not called.
func TestSearchCategoryOutputsNeedsScriptFilters(t *testing.T) {
	c := newSpendTestChain(t, 10, nil)
	category := [32]byte{0xaa}
	reached, err := searchCategoryOutputs(c, [][32]byte{category}, 1, 10,
		func(FoundOutput) bool {
			t.Error("found an output on regular filters")
			return true
		}, nil, nil, nil)
	if reached != 0 || !errors.Is(err, ErrCategoriesNeedScriptFilters) {
		t.Fatalf("up: reached %d, %v", reached, err)
	}

	reached, err = searchCategoryOutputs(c, [][32]byte{category}, 10, 1,
		func(FoundOutput) bool {
			t.Error("found an output on regular filters")
			return true
		}, nil, nil, nil)
	if reached != 11 || !errors.Is(err, ErrCategoriesNeedScriptFilters) {
		t.Fatalf("down: reached %d, %v", reached, err)
	}
}

// TestSearchCategoryOutputsIndex checks that the output reported is the first
// one that carries tokens of a watched category, not the first output.
func TestSearchCategoryOutputsIndex(t *testing.T) {
	category := [32]byte{0xa7}
	tx := scriptTestTx(disputeScript(0x21), wire.OutPoint{Hash: chainhash.Hash{3}, Index: 0})
	tx.AddTxOut(wire.NewTxOut(1000, disputeScript(0x22), wire.TokenData{
		CategoryID: category, Amount: 5, BitField: wire.HAS_AMOUNT,
	}))
	c := newScriptSearchChain(t, 3, map[int32][]*wire.MsgTx{2: {tx}}, nil)

	var found []FoundOutput
	_, err := searchCategoryOutputs(c, [][32]byte{category}, 1, 3,
		func(output FoundOutput) bool {
			found = append(found, output)
			return true
		}, nil, nil, nil)
	if err != nil || len(found) != 1 || found[0].Output != 1 || found[0].Height != 2 {
		t.Fatalf("found %+v, %v", found, err)
	}
}

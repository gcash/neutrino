package neutrino

import (
	"errors"
	"reflect"
	"testing"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// payTestTx returns a transaction with an output to each script.
func payTestTx(id int64, scripts ...[]byte) *wire.MsgTx {
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash{byte(id)}, Index: 7}, nil))
	tx.AddTxOut(wire.NewTxOut(id, []byte{0x51}, wire.TokenData{}))
	for _, script := range scripts {
		tx.AddTxOut(wire.NewTxOut(id, script, wire.TokenData{}))
	}
	return tx
}

var (
	scriptA = []byte{0x76, 0xa9, 0x14, 0xaa, 0x88, 0xac}
	scriptB = []byte{0x76, 0xa9, 0x14, 0xbb, 0x88, 0xac}
)

// TestSearchOutputsBothWays ensures a search finds the transactions that pay to the
// scripts in either direction, in order, and says which output.
func TestSearchOutputsBothWays(t *testing.T) {
	payA, payB := payTestTx(1, scriptA), payTestTx(2, []byte{0x52}, scriptB)
	c := newSpendTestChain(t, 20, map[int32][]*wire.MsgTx{5: {payA}, 12: {payB}})
	for _, test := range []struct {
		from, to, reached int32
		want              []int32
	}{
		{1, 20, 20, []int32{5, 12}},
		{20, 1, 1, []int32{12, 5}},
	} {
		var got []int32
		reached, err := searchOutputs(c, [][]byte{scriptA, scriptB}, test.from, test.to,
			func(f FoundOutput) bool {
				got = append(got, f.Height)
				if f.BlockHash != c.hashes[f.Height] {
					t.Fatalf("block %v at %d", f.BlockHash, f.Height)
				}
				if f.Tx.TxHash() == payB.TxHash() && f.Output != 2 {
					t.Fatalf("output %d of b", f.Output)
				}
				return true
			}, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) || reached != test.reached {
			t.Fatalf("from %d: found at %v, reached %d", test.from, got, reached)
		}
	}
}

// TestSearchOutputsStops ensures a search ends where found says to, and reports
// that height.
func TestSearchOutputsStops(t *testing.T) {
	c := newSpendTestChain(t, 20, map[int32][]*wire.MsgTx{
		15: {payTestTx(1, scriptA)}, 8: {payTestTx(2, scriptA)}})
	var got []int32
	reached, err := searchOutputs(c, [][]byte{scriptA}, 20, 1, func(f FoundOutput) bool {
		got = append(got, f.Height)
		return false
	}, nil, nil, nil)
	if err != nil || reached != 15 || !reflect.DeepEqual(got, []int32{15}) {
		t.Fatalf("found at %v, reached %d, %v", got, reached, err)
	}
}

// TestSearchOutputsFalsePositive ensures a filter that claims a script the block
// doesn't pay to costs a block, and finds nothing.
func TestSearchOutputsFalsePositive(t *testing.T) {
	c := newSpendTestChain(t, 10, map[int32][]*wire.MsgTx{7: {payTestTx(1, scriptA)}})
	key := c.hashes[3]
	falseFilter, err := gcs.BuildGCSFilter(builder.DefaultP, builder.DefaultM,
		builder.DeriveKey(&key), [][]byte{scriptA})
	if err != nil {
		t.Fatal(err)
	}
	c.filters[key] = falseFilter
	var got []int32
	_, err = searchOutputs(c, [][]byte{scriptA}, 1, 10, func(f FoundOutput) bool {
		got = append(got, f.Height)
		return true
	}, nil, nil, nil)
	if err != nil || !reflect.DeepEqual(got, []int32{7}) || !reflect.DeepEqual(c.fetched, []int32{3, 7}) {
		t.Fatalf("found at %v, fetched %v, %v", got, c.fetched, err)
	}
}

// TestSearchOutputsBatches ensures each direction asks for its filters in one go.
func TestSearchOutputsBatches(t *testing.T) {
	for _, test := range []struct {
		from, to int32
		want     optimisticBatchType
	}{{1, 5, forwardBatch}, {5, 1, reverseBatch}} {
		c := newSpendTestChain(t, 5, nil)
		if _, err := searchOutputs(c, [][]byte{scriptA}, test.from, test.to, nil, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		for _, batch := range c.batches {
			if batch != test.want {
				t.Fatalf("from %d: batches %v", test.from, c.batches)
			}
		}
	}
}

// TestSearchOutputsResume ensures a search that stopped says how far it got, for a
// filter no peer sent or a quit, and that one with nothing to look for reads none.
func TestSearchOutputsResume(t *testing.T) {
	c := newSpendTestChain(t, 10, nil)
	c.failAt = 6
	reached, err := searchOutputs(c, [][]byte{scriptA}, 10, 1, nil, nil, nil, nil)
	if err == nil || reached != 7 {
		t.Fatalf("reached %d, %v", reached, err)
	}

	c = newSpendTestChain(t, 10, nil)
	quit := make(chan struct{})
	c.stopAt, c.stop = 4, quit
	reached, err = searchOutputs(c, [][]byte{scriptA}, 1, 10, nil, nil, quit, nil)
	if !errors.Is(err, ErrSearchCancelled) || reached != 4 {
		t.Fatalf("reached %d, %v", reached, err)
	}

	c = newSpendTestChain(t, 10, nil)
	if reached, err := searchOutputs(c, nil, 1, 10, nil, nil, nil, nil); err != nil || reached != 0 || len(c.batches) != 0 {
		t.Fatalf("reached %d, %d filters, %v", reached, len(c.batches), err)
	}
}

// TestSearchOutputsQuitDuringQuery ensures a query a closed quit ended cancels.
func TestSearchOutputsQuitDuringQuery(t *testing.T) {
	c := &quitDuringQuery{spendTestChain: newSpendTestChain(t, 5, nil), quit: make(chan struct{})}
	if _, err := searchOutputs(c, [][]byte{scriptA}, 1, 3, nil, nil, c.quit, nil); !errors.Is(err, ErrSearchCancelled) {
		t.Fatalf("got %v", err)
	}
}

// TestSearchOutputsTxOnce ensures a transaction that pays to more than one of the
// scripts is reported once, with the first of its outputs that does.
func TestSearchOutputsTxOnce(t *testing.T) {
	both := payTestTx(1, scriptB, scriptA)
	c := newSpendTestChain(t, 5, map[int32][]*wire.MsgTx{3: {both}})
	var found []FoundOutput
	_, err := searchOutputs(c, [][]byte{scriptA, scriptB}, 1, 5, func(f FoundOutput) bool {
		found = append(found, f)
		return true
	}, nil, nil, nil)
	if err != nil || len(found) != 1 || found[0].Output != 1 {
		t.Fatalf("%+v, %v", found, err)
	}
}

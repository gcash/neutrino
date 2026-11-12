package neutrino

import (
	"errors"
	"testing"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/neutrino/chainsync"
)

// TestScriptFilterHoldsOutputs checks a block against the script filter built
// for it, which holds the script and the token category of every output, and
// against filters that lack one of them.  The values read from a filter are
// checked against the filter's own matching.
func TestScriptFilterHoldsOutputs(t *testing.T) {
	c := newDisputeChain(t)
	block := wire.NewMsgBlock(&c.block.header)
	for _, tx := range c.block.txs {
		block.Transactions = append(block.Transactions, tx.MsgTx())
	}
	blockHash := block.BlockHash()

	filter, err := scriptFilterOfLeaves(&blockHash, c.leaves)
	if err != nil {
		t.Fatal(err)
	}
	if err := scriptFilterHoldsOutputs(filter, block); err != nil {
		t.Fatalf("the block's own filter: %v", err)
	}

	// Every value read from the filter is a key's, by the filter's own
	// matching, and there is one for each distinct key.
	values, err := gcsFilterValues(filter)
	if err != nil || uint32(len(values)) != filter.N() {
		t.Fatalf("%d values of %d: %v", len(values), filter.N(), err)
	}
	key := builder.DeriveKey(&blockHash)
	distinct := make(map[[32]byte]struct{})
	for _, leaf := range c.leaves {
		distinct[leaf.Key] = struct{}{}
		if match, err := filter.Match(key, leaf.Key[:]); err != nil || !match {
			t.Fatalf("key %x does not match: %v", leaf.Key, err)
		}
	}
	if len(values) != len(distinct) {
		t.Errorf("%d values for %d keys", len(values), len(distinct))
	}

	// A filter without the keys of one kind of leaf in turn: the block is
	// refused when an output's script or category is missing, and passes
	// when only what it spends is, as the block doesn't hold that.
	for _, kind := range []builder.ScriptLeafKind{builder.ScriptLeafOutput,
		builder.ScriptLeafOutputCategory, builder.ScriptLeafSpent} {

		outputKeys := make(map[[32]byte]struct{})
		for _, leaf := range c.leaves {
			if leaf.Kind == builder.ScriptLeafOutput || leaf.Kind == builder.ScriptLeafOutputCategory {
				outputKeys[leaf.Key] = struct{}{}
			}
		}
		for _, drop := range c.leaves {
			if drop.Kind != kind {
				continue
			}
			var kept []builder.ScriptLeaf
			for _, leaf := range c.leaves {
				if leaf.Key != drop.Key {
					kept = append(kept, leaf)
				}
			}
			lacking, err := scriptFilterOfLeaves(&blockHash, kept)
			if err != nil {
				t.Fatal(err)
			}
			_, isOutput := outputKeys[drop.Key]
			err = scriptFilterHoldsOutputs(lacking, block)
			if isOutput != errors.Is(err, errScriptFilterLacksOutput) {
				t.Errorf("filter without key %x of kind %d: %v", drop.Key[:4], kind, err)
			}
		}
	}
}

// TestScriptFilterType checks what differs between the two filter types a
// ChainService can sync: how a served filter is parsed, and the hash and the
// header it gives.
func TestScriptFilterType(t *testing.T) {
	c := newDisputeChain(t)
	blockHash := c.block.header.BlockHash()
	filter, err := scriptFilterOfLeaves(&blockHash, c.leaves)
	if err != nil {
		t.Fatal(err)
	}
	root := builder.ScriptLeavesRoot(c.leaves)
	served, err := builder.ScriptFilterBytes(filter, root)
	if err != nil {
		t.Fatal(err)
	}
	prev := chainhash.Hash{0x77}

	parsed, gotRoot, err := parseServedFilter(wire.GCSFilterScripts, served)
	if err != nil || gotRoot != root {
		t.Fatalf("root %v, %v; want %v", gotRoot, err, root)
	}
	hash, err := servedFilterHash(wire.GCSFilterScripts, parsed, gotRoot)
	if err != nil || hash != chainhash.DoubleHashH(served) {
		t.Errorf("filter hash %v, %v", hash, err)
	}
	header, err := servedFilterHeader(wire.GCSFilterScripts, parsed, gotRoot, prev)
	want := chainhash.DoubleHashH(append(hash[:], prev[:]...))
	if err != nil || header != want {
		t.Errorf("filter header %v, %v; want %v", header, err, want)
	}

	// The same bytes without the root are a regular filter, with another
	// hash.
	regular, noRoot, err := parseServedFilter(wire.GCSFilterRegular, served[32:])
	if err != nil || noRoot != (chainhash.Hash{}) {
		t.Fatalf("regular filter: %v", err)
	}
	regularHash, err := servedFilterHash(wire.GCSFilterRegular, regular, noRoot)
	if err != nil || regularHash == hash {
		t.Errorf("regular filter hash %v, %v", regularHash, err)
	}
	if _, _, err := parseServedFilter(wire.GCSFilterScripts, served[:31]); err == nil {
		t.Error("a script filter shorter than its root parsed")
	}
	if _, _, err := parseServedFilter(wire.FilterType(9), served); err == nil {
		t.Error("an unknown filter type parsed")
	}

	// No script filter headers are hardcoded, so none is refused.
	if err := chainsync.ControlCFHeader(chaincfg.MainNetParams, wire.GCSFilterScripts, 100000, &header); err != nil {
		t.Errorf("script filter header refused: %v", err)
	}
}

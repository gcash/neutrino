package neutrino

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs/builder"
)

// scriptSearchChain is a spendTestChain whose filters are script filters.
type scriptSearchChain struct {
	*spendTestChain
}

func (c *scriptSearchChain) FilterType() wire.FilterType {
	return wire.GCSFilterScripts
}

// newScriptSearchChain returns a chain of height blocks with the transactions
// of txs, and the script filter of each.  scripts gives the scripts of the
// outputs spent that the chain's own transactions don't create.
func newScriptSearchChain(t *testing.T, height int32, txs map[int32][]*wire.MsgTx,
	scripts map[wire.OutPoint][]byte) *scriptSearchChain {

	t.Helper()
	c := newSpendTestChain(t, height, txs)
	outputs := make(map[wire.OutPoint][]byte)
	for outpoint, script := range scripts {
		outputs[outpoint] = script
	}
	for _, block := range c.blocks {
		for _, tx := range block.Transactions {
			hash := tx.TxHash()
			for i, txOut := range tx.TxOut {
				outputs[wire.OutPoint{Hash: hash, Index: uint32(i)}] = txOut.PkScript
			}
		}
	}
	for h, block := range c.blocks {
		var prevOutScripts [][]byte
		for _, tx := range block.Transactions[1:] {
			for _, txIn := range tx.TxIn {
				prevOutScripts = append(prevOutScripts, outputs[txIn.PreviousOutPoint])
			}
		}
		filter, _, err := builder.BuildScriptFilter(block, prevOutScripts)
		if err != nil {
			t.Fatal(err)
		}
		c.filters[c.hashes[h]] = filter
	}
	return &scriptSearchChain{c}
}

// scriptTestTx returns a transaction that spends the outpoints and pays the
// script.
func scriptTestTx(script []byte, spends ...wire.OutPoint) *wire.MsgTx {
	tx := wire.NewMsgTx(2)
	for _, outpoint := range spends {
		tx.AddTxIn(wire.NewTxIn(&outpoint, nil))
	}
	tx.AddTxOut(wire.NewTxOut(1000, script, wire.TokenData{}))
	return tx
}

// TestSearchScriptSpends follows a chain of two spends on script filters,
// which match a spend by the script of the output spent, up and down, and
// checks that a search by outpoints alone is refused there, while on regular
// filters the same search goes by the outpoints.
func TestSearchScriptSpends(t *testing.T) {
	scriptA, scriptB, scriptC := disputeScript(0xa1), disputeScript(0xb2), disputeScript(0xc3)
	first := wire.OutPoint{Hash: chainhash.Hash{1}, Index: 0}
	spendFirst := scriptTestTx(scriptB, first)
	second := wire.OutPoint{Hash: spendFirst.TxHash(), Index: 0}
	spendSecond := scriptTestTx(scriptC, second)
	txs := map[int32][]*wire.MsgTx{
		3: {spendFirst},
		6: {spendSecond},
		// A payment to the first script, which a script filter matches
		// too, and which spends nothing watched.
		8: {scriptTestTx(scriptA, wire.OutPoint{Hash: chainhash.Hash{9}, Index: 0})},
	}
	spent := map[wire.OutPoint][]byte{first: scriptA}

	for _, scriptFilters := range []bool{true, false} {
		var source searchSource
		var chain *spendTestChain
		if scriptFilters {
			c := newScriptSearchChain(t, 10, txs, spent)
			source, chain = c, c.spendTestChain
		} else {
			chain = newSpendTestChain(t, 10, txs)
			source = chain
		}

		var found []FoundSpend
		reached, err := searchScriptSpends(source,
			[]InputWithScript{{OutPoint: first, PkScript: scriptA}}, 1, 10,
			func(spend FoundSpend) []InputWithScript {
				found = append(found, spend)
				if spend.OutPoint == first {
					return []InputWithScript{{OutPoint: second, PkScript: scriptB}}
				}
				return nil
			}, nil, nil, nil)
		if err != nil || reached != 6 || len(found) != 2 ||
			found[0].Height != 3 || found[0].OutPoint != first ||
			found[1].Height != 6 || found[1].OutPoint != second ||
			found[1].SpendingTx.TxHash() != spendSecond.TxHash() {

			t.Fatalf("script filters %v: found %+v, reached %d, %v",
				scriptFilters, found, reached, err)
		}
		// Each matches the two blocks with the spends, and nothing
		// else on the way.
		if !slices.Equal(chain.fetched, []int32{3, 6}) {
			t.Errorf("script filters %v: fetched blocks %v", scriptFilters, chain.fetched)
		}

		// Down, the spend of the first outpoint is found alone.
		found, chain.fetched = nil, nil
		_, err = searchScriptSpends(source,
			[]InputWithScript{{OutPoint: first, PkScript: scriptA}}, 10, 1,
			func(spend FoundSpend) []InputWithScript {
				found = append(found, spend)
				return nil
			}, nil, nil, nil)
		if err != nil || len(found) != 1 || found[0].Height != 3 {
			t.Fatalf("script filters %v, down: found %+v, %v", scriptFilters, found, err)
		}
		// A script filter also matches the block that pays the
		// script, which is fetched and holds no spend.
		wantFetched := []int32{3}
		if scriptFilters {
			wantFetched = []int32{8, 3}
		}
		if !slices.Equal(chain.fetched, wantFetched) {
			t.Errorf("script filters %v, down: fetched blocks %v, want %v",
				scriptFilters, chain.fetched, wantFetched)
		}

		// By outpoints alone.
		found = nil
		reached, err = searchSpends(source, []wire.OutPoint{first}, 1, 10,
			func(spend FoundSpend) []wire.OutPoint {
				found = append(found, spend)
				return nil
			}, nil, nil, nil)
		if scriptFilters {
			if !errors.Is(err, ErrSpendSearchNeedsScripts) || reached != 0 || found != nil {
				t.Errorf("a search by outpoints on script filters: %d, %v", reached, err)
			}
		} else if err != nil || len(found) != 1 {
			t.Errorf("a search by outpoints on regular filters: %+v, %v", found, err)
		}
	}
}

// TestSearchOutputsScriptFilters finds the transaction that pays a script on
// script filters, by the key of the script.
func TestSearchOutputsScriptFilters(t *testing.T) {
	scriptA, scriptB := disputeScript(0xa1), disputeScript(0xb2)
	pay := scriptTestTx(scriptB, wire.OutPoint{Hash: chainhash.Hash{1}, Index: 0})
	c := newScriptSearchChain(t, 6, map[int32][]*wire.MsgTx{4: {pay}}, nil)

	var found []FoundOutput
	reached, err := searchOutputs(c, [][]byte{scriptB}, 1, 6,
		func(output FoundOutput) bool {
			found = append(found, output)
			return true
		}, nil, nil, nil)
	if err != nil || reached != 6 || len(found) != 1 || found[0].Height != 4 ||
		found[0].Tx.TxHash() != pay.TxHash() {

		t.Fatalf("found %+v, reached %d, %v", found, reached, err)
	}
	if !slices.Equal(c.fetched, []int32{4}) {
		t.Errorf("fetched blocks %v", c.fetched)
	}

	// A script no block pays matches no filter.
	c.fetched = nil
	if _, err := searchOutputs(c, [][]byte{scriptA}, 1, 6,
		func(FoundOutput) bool {
			t.Error("found an output of a script no block pays")
			return true
		}, nil, nil, nil); err != nil || len(c.fetched) != 0 {

		t.Errorf("fetched blocks %v, %v", c.fetched, err)
	}
}

// TestScriptFilterWatchList checks what a rescan matches block filters with:
// scripts, outpoints and transaction IDs for regular filters, and the keys of
// scripts and of token categories for script filters, which match a block
// that pays or spends a watched script or carries a watched category.
func TestScriptFilterWatchList(t *testing.T) {
	script := disputeScript(0x42)
	outPoint := wire.OutPoint{Hash: chainhash.Hash{7}, Index: 3}
	category := [32]byte{0xcc}

	regular := &rescanOptions{}
	regular.watchScript(script)
	if err := regular.watchOutPoint(&outPoint); err != nil {
		t.Fatal(err)
	}
	var serialized bytes.Buffer
	_ = outPoint.Serialize(&serialized)
	if len(regular.watchList) != 2 || !bytes.Equal(regular.watchList[0], script) ||
		!bytes.Equal(regular.watchList[1], serialized.Bytes()) {

		t.Errorf("watch list for regular filters: %x", regular.watchList)
	}

	scripts := &rescanOptions{filterType: wire.GCSFilterScripts}
	scripts.watchScript(script)
	if err := scripts.watchOutPoint(&outPoint); err != nil {
		t.Fatal(err)
	}
	scripts.watchCategory(category)
	scriptKey, categoryKey := builder.ScriptKey(script), builder.CategoryKey(category)
	if len(scripts.watchList) != 2 || !bytes.Equal(scripts.watchList[0], scriptKey[:]) ||
		!bytes.Equal(scripts.watchList[1], categoryKey[:]) {

		t.Errorf("watch list for script filters: %x", scripts.watchList)
	}

	// The test block of the dispute tests pays and spends scripts, some
	// with tokens of category cc.
	c := newDisputeChain(t)
	blockHash := c.block.header.BlockHash()
	filter, err := scriptFilterOfLeaves(&blockHash, c.leaves)
	if err != nil {
		t.Fatal(err)
	}
	matches := func(watch func(ro *rescanOptions)) bool {
		ro := &rescanOptions{filterType: wire.GCSFilterScripts}
		watch(ro)
		matched, err := matchBlockFilter(ro, filter, &blockHash)
		if err != nil {
			t.Fatal(err)
		}
		return matched
	}
	for name, want := range map[string]struct {
		watch func(ro *rescanOptions)
		match bool
	}{
		"a script the block pays":      {func(ro *rescanOptions) { ro.watchScript(disputeScript(0x33)) }, true},
		"a script the block spends":    {func(ro *rescanOptions) { ro.watchScript(disputeScript(0x12)) }, true},
		"a category the block carries": {func(ro *rescanOptions) { ro.watchCategory(category) }, true},
		"a script the block lacks":     {func(ro *rescanOptions) { ro.watchScript(script) }, false},
		"a category the block lacks":   {func(ro *rescanOptions) { ro.watchCategory([32]byte{0xdd}) }, false},
		"the raw script, not its key":  {func(ro *rescanOptions) { ro.watchList = [][]byte{disputeScript(0x33)} }, false},
		"an outpoint the block spends": {func(ro *rescanOptions) { _ = ro.watchOutPoint(&c.block.txs[1].MsgTx().TxIn[0].PreviousOutPoint) }, false},
	} {
		// An empty watch list matches nothing.
		if got := matches(want.watch); got != want.match {
			t.Errorf("%s: match %v, want %v", name, got, want.match)
		}
	}

	// A transaction with tokens of a watched category is found, and its
	// output is watched for its spend, once.
	ro := &rescanOptions{filterType: wire.GCSFilterScripts, watchCategories: [][32]byte{category}}
	tokens := c.block.txs[1]
	for range 2 {
		if !ro.carriesWatchedCategory(tokens) {
			t.Fatal("a transaction with tokens of the watched category isn't found")
		}
	}
	wantInput := InputWithScript{
		OutPoint: wire.OutPoint{Hash: *tokens.Hash(), Index: 2},
		PkScript: tokens.MsgTx().TxOut[2].PkScript,
	}
	if len(ro.watchInputs) != 1 || ro.watchInputs[0].OutPoint != wantInput.OutPoint ||
		!bytes.Equal(ro.watchInputs[0].PkScript, wantInput.PkScript) {

		t.Errorf("watched inputs %+v, want %+v", ro.watchInputs, wantInput)
	}
	spender := wire.NewMsgTx(2)
	spender.AddTxIn(wire.NewTxIn(&wantInput.OutPoint, nil))
	if !ro.spendsWatchedInput(bchutil.NewTx(spender)) {
		t.Error("the spend of the output with tokens isn't found")
	}
	if ro.carriesWatchedCategory(c.block.txs[2]) || len(ro.watchList) != 0 {
		t.Error("a transaction without tokens is found, or the script that " +
			"holds tokens is watched")
	}
	if (&rescanOptions{}).carriesWatchedCategory(tokens) {
		t.Error("tokens are found with no category watched")
	}
}

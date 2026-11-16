package neutrino_test

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/integration/rpctest"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/bchwallet/walletdb"
	"github.com/gcash/neutrino"
)

// TestSearchRPAPrefixesNode checks end to end, against a node that serves
// input prefix filters (filter type 2), that a search for the value of an
// input of a payment finds the payment's block and that input, that a short
// prefix of it finds that block too, and that a value no input of the chain
// has finds nothing.
func TestSearchRPAPrefixesNode(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, nil)
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	defer func() { _ = node.TearDown() }()
	if err := node.SetUp(true, 10); err != nil {
		t.Fatalf("unable to set up harness: %v", err)
	}

	payment, err := node.CreateTransaction(
		[]*wire.TxOut{{Value: 100000000, PkScript: []byte{txscript.OP_TRUE}}}, 10, true,
	)
	if err != nil {
		t.Fatalf("unable to create payment: %v", err)
	}
	if _, err := node.Node.SendRawTransaction(payment, true); err != nil {
		t.Fatalf("unable to send payment: %v", err)
	}
	if _, err := node.Node.Generate(1); err != nil {
		t.Fatalf("unable to mine: %v", err)
	}
	_, paymentHeight, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Node.Generate(2); err != nil {
		t.Fatalf("unable to mine: %v", err)
	}
	_, tip, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatal(err)
	}

	// Every value the chain's filters hold, from the node.
	inChain := make(map[uint16]bool)
	for height := int64(0); height <= int64(tip); height++ {
		hash, err := node.Node.GetBlockHash(height)
		if err != nil {
			t.Fatal(err)
		}
		served, err := node.Node.GetCFilter(hash, wire.GCSFilterInputPrefix)
		if err != nil {
			t.Fatalf("no input prefix filter at height %d: %v", height, err)
		}
		values, err := builder.DecodeRPAFilter(served.Data)
		if err != nil {
			t.Fatalf("input prefix filter at height %d: %v", height, err)
		}
		for _, v := range values {
			inChain[v] = true
		}
	}

	dataDir := t.TempDir()
	db, err := walletdb.Create("bdb", filepath.Join(dataDir, "neutrino.db"), true)
	if err != nil {
		t.Fatalf("unable to create db: %v", err)
	}
	defer func() { _ = db.Close() }()
	svc := startScriptFilterService(t, dataDir, db, node)
	defer func() { _ = svc.Stop() }()
	waitForScriptFilterHeaders(t, svc, node)

	search := func(prefix neutrino.RPAPrefix) []neutrino.RPAMatch {
		t.Helper()
		var found []neutrino.RPAMatch
		reached, err := svc.SearchRPAPrefixes([]neutrino.RPAPrefix{prefix}, 1, tip,
			func(m neutrino.RPAMatch) bool {
				found = append(found, m)
				return true
			})
		if err != nil || reached != tip {
			t.Fatalf("search for %x/%d: reached %d of %d, %v", prefix.Value, prefix.Bits, reached, tip, err)
		}
		return found
	}

	value := builder.RPAInputValue(payment.TxIn[0])
	found := search(neutrino.RPAPrefix{Value: value, Bits: 16})
	at := slices.IndexFunc(found, func(m neutrino.RPAMatch) bool { return m.Height == paymentHeight })
	if at < 0 {
		t.Fatalf("the payment's block %d not found: %+v", paymentHeight, found)
	}
	sawInput := false
	for _, in := range found[at].Inputs {
		tx := found[at].Block.MsgBlock().Transactions[in.TxIndex]
		if tx.TxHash() == payment.TxHash() && in.Index == 0 && in.Value == value {
			sawInput = true
		}
	}
	if !sawInput {
		t.Errorf("the payment's input 0 is not among %+v", found[at].Inputs)
	}

	found = search(neutrino.RPAPrefix{Value: value >> 8, Bits: 8})
	if !slices.ContainsFunc(found, func(m neutrino.RPAMatch) bool { return m.Height == paymentHeight }) {
		t.Errorf("an 8-bit prefix of the payment's input didn't find its block: %+v", found)
	}

	absent := uint16(0)
	for inChain[absent] {
		absent++
	}
	if found := search(neutrino.RPAPrefix{Value: absent, Bits: 16}); len(found) != 0 {
		t.Errorf("a value no input of the chain has found %+v", found)
	}
}

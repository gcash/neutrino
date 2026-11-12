package neutrino_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/integration/rpctest"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/bchwallet/walletdb"
	"github.com/gcash/neutrino"
)

// startScriptFilterService starts a ChainService that syncs script filters,
// on the given database, and only connects to the given node.
func startScriptFilterService(t *testing.T, dataDir string, db walletdb.DB,
	node *rpctest.Harness) *neutrino.ChainService {

	t.Helper()

	svc, err := neutrino.NewChainService(neutrino.Config{
		DataDir:      dataDir,
		Database:     db,
		ChainParams:  chaincfg.SimNetParams,
		ConnectPeers: []string{node.P2PAddress()},
		FilterType:   wire.GCSFilterScripts,
	})
	if err != nil {
		t.Fatalf("unable to create ChainService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("unable to start ChainService: %v", err)
	}
	return svc
}

// scriptFilterHeaderOf returns the node's script filter header of a block.
func scriptFilterHeaderOf(t *testing.T, node *rpctest.Harness,
	hash *chainhash.Hash) chainhash.Hash {

	t.Helper()
	header, err := node.Node.GetCFilterHeader(hash, wire.GCSFilterScripts)
	if err != nil {
		t.Fatalf("unable to get the script filter header of %v: %v", hash, err)
	}
	return header.PrevFilterHeader
}

// waitForScriptFilterHeaders waits for the ChainService to sync the node's
// block headers and script filter headers.
func waitForScriptFilterHeaders(t *testing.T, svc *neutrino.ChainService,
	node *rpctest.Harness) {

	t.Helper()

	bestHash, _, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get best block: %v", err)
	}
	want := scriptFilterHeaderOf(t, node, bestHash)

	deadline := time.Now().Add(repairTimeout)
	for {
		best, err := svc.BestBlock()
		if err == nil && best.Hash == *bestHash {
			have, err := svc.ScriptFilterHeaders.FetchHeader(bestHash)
			if err == nil && *have == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the script filter headers "+
				"of block %v", bestHash)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestScriptFilterSync checks end to end that a ChainService set to sync
// script filters (filter type 1) syncs their headers from a node, past a
// filter checkpoint, to the ones the node itself reports; that the filter it
// fetches for a block is the node's, and matches the script the block pays
// and the script of the output it spends; that the block passes the check
// against its filter; and that the headers are kept across a restart.
func TestScriptFilterSync(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, []string{"--txindex"})
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	defer func() { _ = node.TearDown() }()
	if err := node.SetUp(true, 10); err != nil {
		t.Fatalf("unable to set up harness: %v", err)
	}

	script := []byte{txscript.OP_TRUE}
	payment, err := node.CreateTransaction(
		[]*wire.TxOut{{Value: 100000000, PkScript: script}}, 10, true,
	)
	if err != nil {
		t.Fatalf("unable to create payment: %v", err)
	}
	if _, err := node.Node.SendRawTransaction(payment, true); err != nil {
		t.Fatalf("unable to send payment: %v", err)
	}
	hashes, err := node.Node.Generate(1)
	if err != nil {
		t.Fatalf("unable to mine: %v", err)
	}
	paymentBlock := hashes[0]
	_, paymentHeight, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatal(err)
	}

	// Enough blocks on top that the filter headers are synced through a
	// filter checkpoint, which has its own requests.
	if _, err := node.Node.Generate(wire.CFCheckptInterval); err != nil {
		t.Fatalf("unable to mine: %v", err)
	}
	bestHash, tip, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatal(err)
	}
	if tip <= wire.CFCheckptInterval {
		t.Fatalf("the chain ends at %d, before a filter checkpoint", tip)
	}

	dataDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dataDir, "neutrino.db"), true,
	)
	if err != nil {
		t.Fatalf("unable to create db: %v", err)
	}
	defer func() { _ = db.Close() }()
	svc := startScriptFilterService(t, dataDir, db, node)
	stopped := false
	defer func() {
		if !stopped {
			_ = svc.Stop()
		}
	}()
	waitForScriptFilterHeaders(t, svc, node)

	if svc.FilterType() != wire.GCSFilterScripts || svc.RegFilterHeaders != nil ||
		svc.FilterHeaders() != svc.ScriptFilterHeaders {

		t.Fatalf("the service doesn't keep script filter headers only")
	}

	// The filter headers are the node's, at the start, around the payment,
	// around the checkpoint and at the tip.
	for _, height := range []int64{0, 1, int64(paymentHeight), wire.CFCheckptInterval - 1,
		wire.CFCheckptInterval, wire.CFCheckptInterval + 1, int64(tip)} {

		hash, err := node.Node.GetBlockHash(height)
		if err != nil {
			t.Fatal(err)
		}
		have, err := svc.ScriptFilterHeaders.FetchHeaderByHeight(uint32(height))
		if err != nil {
			t.Fatalf("no script filter header at height %d: %v", height, err)
		}
		if want := scriptFilterHeaderOf(t, node, hash); *have != want {
			t.Errorf("script filter header at height %d: %v, want %v",
				height, have, want)
		}
	}

	// The filter of the block with the payment is the one the node
	// serves, without the root in front of it.
	filter, err := svc.GetCFilter(*paymentBlock, wire.GCSFilterScripts)
	if err != nil {
		t.Fatalf("unable to get the script filter: %v", err)
	}
	served, err := node.Node.GetCFilter(paymentBlock, wire.GCSFilterScripts)
	if err != nil {
		t.Fatalf("unable to get the node's script filter: %v", err)
	}
	nodeFilter, _, err := builder.ParseScriptFilter(served.Data)
	if err != nil {
		t.Fatalf("unable to parse the node's script filter: %v", err)
	}
	got, _ := filter.NBytes()
	want, _ := nodeFilter.NBytes()
	if !bytes.Equal(got, want) {
		t.Errorf("script filter %x, want the node's %x", got, want)
	}

	// It matches the script the payment pays and the script of the output
	// the payment spends, and not a script the block doesn't have.
	prevOut := payment.TxIn[0].PreviousOutPoint
	prevTx, err := node.Node.GetRawTransaction(&prevOut.Hash)
	if err != nil {
		t.Fatalf("unable to get the spent transaction: %v", err)
	}
	spentScript := prevTx.MsgTx().TxOut[prevOut.Index].PkScript
	key := builder.DeriveKey(paymentBlock)
	for name, c := range map[string]struct {
		script []byte
		want   bool
	}{
		"the script paid":       {script, true},
		"the script spent":      {spentScript, true},
		"a script not in block": {[]byte{txscript.OP_2, txscript.OP_3}, false},
	} {
		scriptKey := builder.ScriptKey(c.script)
		match, err := filter.Match(key, scriptKey[:])
		if err != nil || match != c.want {
			t.Errorf("%s: match %v, %v; want %v", name, match, err, c.want)
		}
	}

	// A filter of the type that isn't synced has no filter headers to be
	// checked against.
	if _, err := svc.GetCFilter(*paymentBlock, wire.GCSFilterRegular); err == nil {
		t.Error("got a regular filter from a service that syncs script filters")
	}

	// The block is fetched and passes the check against its filter.
	block, err := svc.GetBlock(*paymentBlock)
	if err != nil {
		t.Fatalf("unable to get the block: %v", err)
	}
	if *block.Hash() != *paymentBlock {
		t.Fatalf("got block %v, want %v", block.Hash(), paymentBlock)
	}

	// After a restart the headers are read back from their own store.
	if err := svc.Stop(); err != nil {
		t.Fatalf("unable to stop the ChainService: %v", err)
	}
	stopped = true
	svc = startScriptFilterService(t, dataDir, db, node)
	defer func() { _ = svc.Stop() }()
	have, err := svc.ScriptFilterHeaders.FetchHeader(bestHash)
	if err != nil {
		t.Fatalf("no script filter header of the tip after a restart: %v", err)
	}
	if want := scriptFilterHeaderOf(t, node, bestHash); *have != want {
		t.Errorf("script filter header of the tip after a restart: %v, "+
			"want %v", have, want)
	}
}

// TestScriptFilterPeerProbe checks that a ChainService set to sync script
// filters disconnects a node that doesn't serve them, which can't be told
// from its services, and syncs no filter header from it.
func TestScriptFilterPeerProbe(t *testing.T) {
	node, err := rpctest.New(
		&chaincfg.SimNetParams, nil, []string{"--noscriptfilters"},
	)
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	defer func() { _ = node.TearDown() }()
	if err := node.SetUp(true, 10); err != nil {
		t.Fatalf("unable to set up harness: %v", err)
	}

	dataDir := t.TempDir()
	db, err := walletdb.Create(
		"bdb", filepath.Join(dataDir, "neutrino.db"), true,
	)
	if err != nil {
		t.Fatalf("unable to create db: %v", err)
	}
	defer func() { _ = db.Close() }()
	svc := startScriptFilterService(t, dataDir, db, node)
	defer func() { _ = svc.Stop() }()

	// It connects, as the node serves filters by its services, and then
	// drops the node once the node hasn't answered for script filters.
	connected, dropped := false, false
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) && !dropped {
		if svc.ConnectedCount() > 0 {
			connected = true
		} else if connected {
			dropped = true
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !connected || !dropped {
		t.Fatalf("connected %v, dropped %v", connected, dropped)
	}
	if _, height, err := svc.ScriptFilterHeaders.ChainTip(); err != nil || height != 0 {
		t.Errorf("script filter headers synced to height %d from a node "+
			"that doesn't serve them: %v", height, err)
	}
}

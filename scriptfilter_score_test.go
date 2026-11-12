package neutrino

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/integration/rpctest"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchwallet/walletdb"
)

// setMirrorScore sets the persistent and transient parts of m directly.  A
// mirror counts a request only when the result still fits, so a test that
// needs a score to start high, and to stay there, writes the persistent part
// here.  That part does not decay.
func setMirrorScore(m *banScoreMirror, persistent, transient uint32) {
	m.score.Increase(persistent, transient)
}

// TestTakeBanScore checks that a request is counted toward the ban score a
// peer keeps for us while the score has room for it, that one the score has
// no room for waits, and that the wait ends when the score decays, the peer
// leaves, or we shut down.
func TestTakeBanScore(t *testing.T) {
	const tick = 10 * time.Millisecond
	here := func() bool { return false }
	never := make(chan struct{})

	// A fresh score takes requests at once and counts each.  What is
	// counted decays, a little within a second, so one may be gone again.
	var fresh banScoreMirror
	for i := 0; i < 5; i++ {
		if !takeBanScore(&fresh, 1, tick, here, never) {
			t.Fatalf("a fresh score did not take request %d", i+1)
		}
	}
	if got := fresh.Int(); got != 4 && got != 5 {
		t.Fatalf("score %d after five requests, want 5, or 4 a second later", got)
	}

	// The last request the score has room for is taken.  The persistent
	// part of a score doesn't decay, so the scores below stay as they are.
	var last banScoreMirror
	setMirrorScore(&last, maxMempoolBanScore-1, 0)
	if !takeBanScore(&last, 1, tick, here, never) {
		t.Fatal("the last request the score has room for was not taken")
	}

	// A request the score has no room for waits, and gives up when we
	// shut down, without counting.
	var full banScoreMirror
	setMirrorScore(&full, maxMempoolBanScore, 0)
	quit := make(chan struct{})
	result := make(chan bool, 1)
	go func() { result <- takeBanScore(&full, 1, tick, here, quit) }()
	select {
	case <-result:
		t.Fatal("a request the score has no room for did not wait")
	case <-time.After(20 * tick):
	}
	close(quit)
	select {
	case took := <-result:
		if took {
			t.Fatal("a request was taken as we shut down")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a waiting request did not end when we shut down")
	}
	if got := full.Int(); got != maxMempoolBanScore {
		t.Fatalf("score %d after a request that was not taken, want %d", got,
			maxMempoolBanScore)
	}

	// It gives up when the peer has left, even with room in the score.
	var left banScoreMirror
	gone := func() bool { return true }
	if takeBanScore(&left, 1, tick, gone, never) {
		t.Fatal("a request was taken for a peer that left")
	}
	if got := left.Int(); got != 0 {
		t.Fatalf("score %d after a request to a peer that left, want 0", got)
	}

	// A score that decays makes room: the transient part halves every
	// minute, counted in whole seconds, so the next second is enough here.
	var decaying banScoreMirror
	setMirrorScore(&decaying, 0, maxMempoolBanScore)
	go func() { result <- takeBanScore(&decaying, 1, tick, here, never) }()
	select {
	case took := <-result:
		if !took {
			t.Fatal("a request was not taken once the score decayed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a request still waited after the score decayed")
	}
	if got := decaying.Int(); got > maxMempoolBanScore {
		t.Fatalf("score %d is over the %d we allow", got, maxMempoolBanScore)
	}
}

// TestScriptTreeRequestsCount checks, against a node, that each proof and each
// range of leaves we ask a peer for is counted toward the ban score we mirror
// for it.
func TestScriptTreeRequestsCount(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, nil)
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	defer func() { _ = node.TearDown() }()
	if err := node.SetUp(true, 1); err != nil {
		t.Fatalf("unable to set up harness: %v", err)
	}
	bestHash, _, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get best block: %v", err)
	}

	dataDir := t.TempDir()
	db, err := walletdb.Create("bdb", filepath.Join(dataDir, "neutrino.db"), true)
	if err != nil {
		t.Fatalf("unable to create db: %v", err)
	}
	defer func() { _ = db.Close() }()
	svc, err := NewChainService(Config{
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
	defer func() { _ = svc.Stop() }()

	var sp *ServerPeer
	deadline := time.Now().Add(30 * time.Second)
	for sp == nil {
		for _, peer := range svc.Peers() {
			if peer.Connected() {
				sp = peer
			}
		}
		if sp == nil {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for the node as a peer")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// The block only has its coinbase, whose output is the one leaf.
	const each = 3
	for i := 0; i < each; i++ {
		if svc.fetchScriptProof(sp.Addr(), *bestHash, 0, svc.quit) == nil {
			t.Fatalf("no cfproof for request %d", i+1)
		}
		if svc.fetchScriptRange(sp.Addr(), *bestHash, 0, [32]byte{}, svc.quit) == nil {
			t.Fatalf("no cfrange for request %d", i+1)
		}
	}
	// What is counted decays, by less than one in the seconds this takes.
	want := uint32(2 * each * scriptTreeBanScore)
	if got := sp.mempoolScore.Int(); got != want && got != want-1 {
		t.Fatalf("score %d after %d proofs and %d ranges, want %d", got, each,
			each, want)
	}
}

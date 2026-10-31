package neutrino

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/peer"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// mempoolTestPeer is a remote peer that serves the filter of its mempool and
// counts the requests for it and for its mempool.
type mempoolTestPeer struct {
	sp         *ServerPeer
	getFilter  int32
	getMempool int32
	ping       chan struct{}
}

// connectMempoolPeer connects a peer of s that advertises services, runs the
// given version of bchd, and serves a filter of its mempool that holds
// entries, or disconnects when asked for it if drop is set.
func connectMempoolPeer(t *testing.T, s *ChainService,
	services wire.ServiceFlag, version string, entries [][]byte,
	drop bool) *mempoolTestPeer {

	t.Helper()

	filter, err := gcs.BuildGCSFilter(builder.DefaultP, builder.DefaultM,
		builder.DeriveKey(&chainhash.Hash{}), entries)
	if err != nil {
		t.Fatal(err)
	}
	filterBytes, err := filter.NBytes()
	if err != nil {
		t.Fatal(err)
	}

	p := &mempoolTestPeer{ping: make(chan struct{}, 1)}
	p.sp = connectTestPeer(t, s, services, peer.MessageListeners{
		OnGetCFMempool: func(remote *peer.Peer, _ *wire.MsgGetCFMempool) {
			atomic.AddInt32(&p.getFilter, 1)
			if drop {
				remote.Disconnect()
				return
			}
			remote.QueueMessage(wire.NewMsgCFilter(
				wire.GCSFilterRegular, &chainhash.Hash{},
				filterBytes,
			), nil)
		},
		OnMemPool: func(*peer.Peer, *wire.MsgMemPool) {
			atomic.AddInt32(&p.getMempool, 1)
		},
		OnPing: func(*peer.Peer, *wire.MsgPing) {
			select {
			case p.ping <- struct{}{}:
			default:
			}
		},
	}, func(cfg *peer.Config) {
		// As bchd names itself.
		cfg.UserAgentName = "/bchd"
		cfg.UserAgentVersion = version
	})
	return p
}

// requests returns the requests for the filter of its mempool and for its
// mempool that the peer received, once those sent before have arrived.
func (p *mempoolTestPeer) requests(t *testing.T) (int32, int32) {
	t.Helper()

	// Messages arrive in order, so once the ping is in, so is anything
	// sent before it.
	p.sp.QueueMessage(wire.NewMsgPing(1), nil)
	select {
	case <-p.ping:
	case <-time.After(10 * time.Second):
		t.Fatal("ping timed out")
	}
	return atomic.LoadInt32(&p.getFilter), atomic.LoadInt32(&p.getMempool)
}

// newTestMempoolRequester returns a requester that asks the peers, as synced
// tells it whether the block headers are synced.
func newTestMempoolRequester(t *testing.T, synced *atomic.Bool,
	peers ...*mempoolTestPeer) *mempoolRequester {

	quit := make(chan struct{})
	t.Cleanup(func() { close(quit) })
	r := newMempoolRequester(func() []*ServerPeer {
		sps := make([]*ServerPeer, len(peers))
		for i, p := range peers {
			sps[i] = p.sp
		}
		return sps
	}, synced.Load, quit)
	r.timeout = 5 * time.Second
	return r
}

// wantsMempool returns whether a request for the mempool is still wanted.
func (r *mempoolRequester) wantsMempool() bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	return r.wanted
}

// testMempoolEntries returns the entries that the filter of a mempool holds
// for a payment to a script and a spend of an outpoint.
func testMempoolEntries(t *testing.T) (script, outPoint []byte) {
	t.Helper()

	script = []byte{0x76, 0xa9, 0x14, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
		12, 13, 14, 15, 16, 17, 18, 19, 20, 0x88, 0xac}
	var buf bytes.Buffer
	op := wire.OutPoint{Hash: chainhash.Hash{7}, Index: 3}
	if err := op.Serialize(&buf); err != nil {
		t.Fatal(err)
	}
	return script, buf.Bytes()
}

// TestMempoolBanScores checks that the ban scores counted for a request for
// the filter of the mempool and for the mempool are bchd's from 0.22.5 on,
// and the old ones for an older version, a pre-release, or a peer whose
// version can't be told.
func TestMempoolBanScores(t *testing.T) {
	tests := []struct {
		userAgent       string
		filter, mempool uint32
	}{
		{"/bchd:0.22.5/", 10, 20},
		{"/bchd:0.23.0/", 10, 20},
		{"/bchd:1.0.0/", 10, 20},
		{"/bchd:0.22.5(node one)/", 10, 20},
		{"/bchd:0.22.4/", 33, 33},
		{"/bchd:0.21.9/", 33, 33},
		{"/bchd:0.22.5-beta/", 33, 33},
		{"bchd:0.22.5/", 33, 33},
		{"/Bitcoin Cash Node:28.0.0(EB32.0)/", 33, 33},
		{"", 33, 33},
	}
	for _, test := range tests {
		filter, mempool := mempoolBanScores(test.userAgent)
		if filter != test.filter || mempool != test.mempool {
			t.Errorf("%q: scores %d and %d, want %d and %d",
				test.userAgent, filter, mempool, test.filter,
				test.mempool)
		}
	}
}

// TestMempoolRequestMatches checks that the mempool is asked for when the
// filter of the mempool holds a script or an outpoint we watch, and only
// then, and that the request is done either way once the filter came.
func TestMempoolRequestMatches(t *testing.T) {
	script, outPoint := testMempoolEntries(t)
	other := []byte("someone else's script")

	tests := []struct {
		name    string
		holds   [][]byte
		mempool int32
	}{
		{"pays a script we watch", [][]byte{other, script}, 1},
		{"spends an outpoint we watch", [][]byte{outPoint, other}, 1},
		{"neither", [][]byte{other}, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, _ := newPeerTestService(t)
			p := connectMempoolPeer(t, s, wire.SFNodeCF|wire.SFNodeBloom,
				"0.22.5", test.holds, false)
			var synced atomic.Bool
			synced.Store(true)
			r := newTestMempoolRequester(t, &synced, p)

			r.request([][]byte{script, outPoint})
			r.attempt()
			getFilter, getMempool := p.requests(t)
			if getFilter != 1 || getMempool != test.mempool {
				t.Fatalf("asked for the filter %d times and the "+
					"mempool %d, want once and %d", getFilter,
					getMempool, test.mempool)
			}
			if r.wantsMempool() {
				t.Fatal("still wants the mempool after the filter came")
			}
		})
	}
}

// TestMempoolRequestWaits checks that a request isn't sent before the block
// headers are synced, as the transactions that the reply announces wouldn't be
// fetched, nor to a peer that doesn't serve bloom filters, which bchd requires
// for a mempool message, and that it waits rather than being dropped.
func TestMempoolRequestWaits(t *testing.T) {
	script, _ := testMempoolEntries(t)
	s, _ := newPeerTestService(t)
	cfOnly := connectMempoolPeer(t, s, wire.SFNodeCF, "0.22.5",
		[][]byte{script}, false)
	bloom := connectMempoolPeer(t, s, wire.SFNodeCF|wire.SFNodeBloom,
		"0.22.5", [][]byte{script}, false)
	var synced atomic.Bool
	r := newTestMempoolRequester(t, &synced, cfOnly, bloom)

	r.request([][]byte{script})
	r.attempt()
	if getFilter, _ := bloom.requests(t); getFilter != 0 {
		t.Fatal("asked for the filter before the headers were synced")
	}
	if !r.wantsMempool() {
		t.Fatal("dropped the request before the headers were synced")
	}

	synced.Store(true)
	r.attempt()
	if getFilter, _ := cfOnly.requests(t); getFilter != 0 {
		t.Fatal("asked a peer without bloom filters for the filter")
	}
	if getFilter, getMempool := bloom.requests(t); getFilter != 1 ||
		getMempool != 1 {

		t.Fatalf("asked the bloom peer for the filter %d times and the "+
			"mempool %d, want once each", getFilter, getMempool)
	}
}

// TestMempoolRequestBanScore checks that a peer is asked only while its ban
// score for us leaves room for the request, by bchd's scores for its version,
// that another peer is asked meanwhile, and that a request no peer has room
// for waits.
func TestMempoolRequestBanScore(t *testing.T) {
	script, _ := testMempoolEntries(t)
	s, _ := newPeerTestService(t)
	services := wire.SFNodeCF | wire.SFNodeBloom
	holds := [][]byte{script}

	// An old bchd adds 33 for each request, so a peer has room for one
	// full request.
	oldA := connectMempoolPeer(t, s, services, "0.22.4", holds, false)
	oldB := connectMempoolPeer(t, s, services, "0.22.4", holds, false)
	var synced atomic.Bool
	synced.Store(true)
	r := newTestMempoolRequester(t, &synced, oldA, oldB)
	for i := 0; i < 3; i++ {
		r.request([][]byte{script})
		r.attempt()
	}
	for _, p := range []*mempoolTestPeer{oldA, oldB} {
		if getFilter, getMempool := p.requests(t); getFilter != 1 ||
			getMempool != 1 {

			t.Fatalf("asked an old peer for the filter %d times and "+
				"the mempool %d, want once each", getFilter,
				getMempool)
		}
	}
	if !r.wantsMempool() {
		t.Fatal("dropped the request that no peer had room for")
	}

	// bchd 0.22.5 adds 10 and 20, so a peer has room for three.
	current := connectMempoolPeer(t, s, services, "0.22.5", holds, false)
	r = newTestMempoolRequester(t, &synced, current)
	for i := 0; i < 4; i++ {
		r.request([][]byte{script})
		r.attempt()
	}
	if getFilter, getMempool := current.requests(t); getFilter != 3 ||
		getMempool != 3 {

		t.Fatalf("asked a current peer for the filter %d times and the "+
			"mempool %d, want 3 each", getFilter, getMempool)
	}
	if !r.wantsMempool() {
		t.Fatal("dropped the request that no peer had room for")
	}
}

// TestMempoolRequestDropped checks that a peer that disconnects right after a
// request while another stays connected isn't asked again, nor any other peer
// at its host, as it was taken to have banned us, and that the request goes to
// another peer and waits rather than being dropped.
func TestMempoolRequestDropped(t *testing.T) {
	script, _ := testMempoolEntries(t)
	s, _ := newPeerTestService(t)
	services := wire.SFNodeCF | wire.SFNodeBloom
	dropping := connectMempoolPeer(t, s, services, "0.22.5",
		[][]byte{script}, true)
	other := connectMempoolPeer(t, s, services, "0.22.5",
		[][]byte{script}, false)
	var synced atomic.Bool
	synced.Store(true)
	r := newTestMempoolRequester(t, &synced, dropping, other)
	r.timeout = 500 * time.Millisecond
	r.dropWindow = 100 * time.Millisecond

	// The dropping peer, listed first, sends no filter, so the other is
	// asked.
	r.request([][]byte{script})
	r.attempt()
	if getFilter, getMempool := other.requests(t); getFilter != 1 ||
		getMempool != 1 {

		t.Fatalf("asked the other peer for the filter %d times and the "+
			"mempool %d, want once each", getFilter, getMempool)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !r.droppedUs(dropping.sp) {
		if time.Now().After(deadline) {
			t.Fatal("a peer that disconnected right after a request " +
				"wasn't taken to have banned us")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The other peer is on the same host.
	r.request([][]byte{script})
	r.attempt()
	if getFilter, _ := other.requests(t); getFilter != 1 {
		t.Fatal("asked a peer at the host of one that banned us")
	}
	if !r.wantsMempool() {
		t.Fatal("dropped the request")
	}
}

// TestMempoolRequestNetworkDropped checks that a peer that disconnects right
// after a request along with every other peer, as when the network drops,
// isn't taken to have banned us.
func TestMempoolRequestNetworkDropped(t *testing.T) {
	script, _ := testMempoolEntries(t)
	s, _ := newPeerTestService(t)
	dropping := connectMempoolPeer(t, s, wire.SFNodeCF|wire.SFNodeBloom,
		"0.22.5", [][]byte{script}, true)
	var synced atomic.Bool
	synced.Store(true)
	r := newTestMempoolRequester(t, &synced, dropping)
	r.timeout = 500 * time.Millisecond
	r.dropWindow = 100 * time.Millisecond

	r.request([][]byte{script})
	r.attempt()
	time.Sleep(10 * r.dropWindow)
	if r.droppedUs(dropping.sp) {
		t.Fatal("our only peer, which disconnected, was taken to have " +
			"banned us")
	}
}

// TestMempoolRequestWake checks that the mempool is asked for again when the
// wall clock shows the process was suspended, as a phone suspends it when it
// sleeps.
func TestMempoolRequestWake(t *testing.T) {
	script, _ := testMempoolEntries(t)
	s, _ := newPeerTestService(t)
	p := connectMempoolPeer(t, s, wire.SFNodeCF|wire.SFNodeBloom, "0.22.5",
		[][]byte{script}, false)
	var synced atomic.Bool
	synced.Store(true)
	r := newTestMempoolRequester(t, &synced, p)
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	r.now = func() time.Time { return time.Unix(0, clock.Load()) }
	go r.run()

	waitForFilterRequests := func(n int32) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for atomic.LoadInt32(&p.getFilter) < n {
			if time.Now().After(deadline) {
				t.Fatalf("asked for the filter %d times, want %d",
					atomic.LoadInt32(&p.getFilter), n)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	r.request([][]byte{script})
	waitForFilterRequests(1)

	// A minute passes by the wall clock while the process sleeps.
	clock.Add(int64(time.Minute))
	waitForFilterRequests(2)
}

// TestTxGetData checks that transactions are asked for at most
// maxGetDataTxs to a getdata message, which bchd adds nothing to a peer's ban
// score for.
func TestTxGetData(t *testing.T) {
	var invVects []*wire.InvVect
	for i := 0; i < 1200; i++ {
		invVects = append(invVects, wire.NewInvVect(wire.InvTypeTx,
			&chainhash.Hash{byte(i), byte(i >> 8)}))
	}
	msgs := txGetData(invVects)
	if len(msgs) != 3 || len(msgs[0].InvList) != 505 ||
		len(msgs[1].InvList) != 505 || len(msgs[2].InvList) != 190 {

		t.Fatalf("got %d messages", len(msgs))
	}
	if msgs[2].InvList[189].Hash != invVects[1199].Hash {
		t.Fatal("the last transaction isn't the last asked for")
	}
	if len(txGetData(nil)) != 0 {
		t.Fatal("asked for no transactions in a message")
	}
}

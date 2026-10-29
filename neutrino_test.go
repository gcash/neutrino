package neutrino

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gcash/bchd/addrmgr"
	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/connmgr"
	"github.com/gcash/bchd/peer"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
	"github.com/gcash/neutrino/banman"
)

// noBans is a ban store that has banned no one.
type noBans struct{}

func (noBans) BanIPNet(*net.IPNet, banman.Reason, time.Duration) error { return nil }

func (noBans) Status(*net.IPNet) (banman.Status, error) { return banman.Status{}, nil }

// newTestService returns a ChainService with an empty address manager and a
// stopped connection manager, for tests that need no peer handler: with quit
// closed, the service reports no connected peers and no outbound groups.
func newTestService(t *testing.T) *ChainService {
	quit := make(chan struct{})
	close(quit)
	cmgr, err := connmgr.New(&connmgr.Config{
		Dial: func(net.Addr) (net.Conn, error) {
			return nil, errors.New("no dials in tests")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cmgr.Stop()
	return &ChainService{
		chainParams: chaincfg.MainNetParams,
		addrManager: addrmgr.New(t.TempDir(), nil),
		connManager: cmgr,
		banStore:    noBans{},
		query:       make(chan interface{}),
		quit:        quit,
		nameResolver: func(host string) ([]net.IP, error) {
			return []net.IP{net.ParseIP(host)}, nil
		},
	}
}

// TestNewAddressAfterFailedDials checks that newAddress counts an address it
// picks as tried, so that it doesn't pick one whose dial failed again within
// ten minutes while there are addresses it hasn't tried.
func TestNewAddressAfterFailedDials(t *testing.T) {
	s := newTestService(t)
	var addrs []*wire.NetAddressV2
	for i := 0; i < 100; i++ {
		addrs = append(addrs, wire.NewNetAddressV2IPPort(
			net.IPv4(52, byte(i), 0, 1), 8333, RequiredServices))
	}
	s.addrManager.AddAddresses(addrs, addrs[0])

	// No dial succeeds, so only newAddress can mark the attempts.  Thirty
	// random picks of a hundred addresses would repeat one almost always.
	picked := make(map[string]bool)
	for i := 0; i < 30; i++ {
		addr, err := s.newAddress()
		if err != nil {
			t.Fatal(err)
		}
		if picked[addr.String()] {
			t.Fatalf("pick %d: %v again, whose dial failed", i, addr)
		}
		picked[addr.String()] = true
	}
}

// TestNewAddressAfterLostPeer checks that newAddress picks the peer the service
// just lost, as to a network change, among addresses it never tried, instead of
// passing it over for ten minutes because it was attempted when it connected.
func TestNewAddressAfterLostPeer(t *testing.T) {
	s := newTestService(t)
	lost := wire.NewNetAddressV2IPPort(net.ParseIP("35.208.165.58"), 8333,
		RequiredServices)
	addrs := []*wire.NetAddressV2{lost}
	for i := 0; i < 60; i++ {
		addrs = append(addrs, wire.NewNetAddressV2IPPort(
			net.IPv4(52, byte(i), 0, 1), 8333, RequiredServices))
	}
	s.addrManager.AddAddresses(addrs, lost)

	// The peer connected a moment ago: newAddress marked the attempt, and
	// the handshake marked it good.
	s.addrManager.Attempt(lost)
	s.addrManager.Good(lost)

	// Each pick returns the lost peer or an address never tried, about
	// evenly, so 20 picks miss it about once in a million runs.
	for i := 0; i < 20; i++ {
		addr, err := s.newAddress()
		if err != nil {
			t.Fatal(err)
		}
		if addr.String() == "35.208.165.58:8333" {
			return
		}
	}
	t.Fatal("newAddress passed over the peer just lost for 20 picks")
}

// badAddr is an address that the peer package can't split into a host and a
// port.
type badAddr struct{}

func (badAddr) Network() string { return "tcp" }

func (badAddr) String() string { return "no-port" }

// TestOutboundPeerConnectedBadAddress checks that outboundPeerConnected drops a
// connection whose peer can't be created, instead of going on with a nil peer.
func TestOutboundPeerConnectedBadAddress(t *testing.T) {
	s := newTestService(t)
	conn, other := net.Pipe()
	defer func() {
		_ = conn.Close()
		_ = other.Close()
	}()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("outboundPeerConnected panicked: %v", r)
		}
	}()
	s.outboundPeerConnected(&connmgr.ConnReq{Addr: badAddr{}}, conn)
}

// recordedBans is a ban store that records the reason of each ban.
type recordedBans struct {
	mtx     sync.Mutex
	reasons map[string]banman.Reason
}

func (r *recordedBans) BanIPNet(ipNet *net.IPNet, reason banman.Reason,
	_ time.Duration) error {

	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.reasons[ipNet.String()] = reason
	return nil
}

func (r *recordedBans) Status(*net.IPNet) (banman.Status, error) {
	return banman.Status{}, nil
}

func (r *recordedBans) bans() map[string]banman.Reason {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	bans := make(map[string]banman.Reason, len(r.reasons))
	for ipNet, reason := range r.reasons {
		bans[ipNet] = reason
	}
	return bans
}

// newPeerTestService returns a ChainService that can complete version
// handshakes with peers, recording the bans it makes.
func newPeerTestService(t *testing.T) (*ChainService, *recordedBans) {
	quit := make(chan struct{})
	t.Cleanup(func() { close(quit) })
	bans := &recordedBans{reasons: make(map[string]banman.Reason)}
	return &ChainService{
		chainParams: chaincfg.SimNetParams,
		addrManager: addrmgr.New(t.TempDir(), nil),
		banStore:    bans,
		timeSource:  blockchain.NewMedianTime(),
		services:    Services,
		newPeers:    make(chan *ServerPeer, 1),
		query:       make(chan interface{}),
		quit:        quit,
	}, bans
}

// connectTestPeer connects an outbound peer of s to a remote peer on the
// loopback interface that advertises services and uses listeners, and returns
// it once the version handshake is done.
func connectTestPeer(t *testing.T, s *ChainService, services wire.ServiceFlag,
	listeners peer.MessageListeners) *ServerPeer {

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		remote := peer.NewInboundPeer(&peer.Config{
			ChainParams:            &s.chainParams,
			Services:               services,
			Listeners:              listeners,
			TstAllowSelfConnection: true,
		})
		remote.AssociateConnection(conn)
	}()

	sp := newServerPeer(s, false)
	cfg := newPeerConfig(sp)
	cfg.NewestBlock = nil
	cfg.TstAllowSelfConnection = true
	sp.Peer, err = peer.NewOutboundPeer(cfg, ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	sp.AssociateConnection(conn)
	t.Cleanup(sp.Disconnect)

	// The verack that hands the peer to the server follows the version.
	select {
	case added := <-s.newPeers:
		if added != sp {
			t.Fatal("unexpected peer added")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("version handshake timed out")
	}
	return sp
}

// TestOnVersionServices checks that a peer is banned for lacking compact
// filters only when it doesn't advertise SFNodeCF, so that peers serving
// filters without bloom filter support stay usable.
func TestOnVersionServices(t *testing.T) {
	tests := []struct {
		name     string
		services wire.ServiceFlag
		banned   bool
	}{
		{"cf", wire.SFNodeNetwork | wire.SFNodeCF, false},
		{"cf and bloom", wire.SFNodeNetwork | wire.SFNodeCF |
			wire.SFNodeBloom, false},
		{"bloom", wire.SFNodeNetwork | wire.SFNodeBloom, true},
		{"none", wire.SFNodeNetwork, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, bans := newPeerTestService(t)
			sp := connectTestPeer(t, s, test.services,
				peer.MessageListeners{})
			if sp.Services() != test.services {
				t.Fatalf("services %v, want %v", sp.Services(),
					test.services)
			}

			got := bans.bans()
			if !test.banned {
				if len(got) != 0 {
					t.Fatalf("unexpected bans %v", got)
				}
				return
			}
			if len(got) != 1 || got["127.0.0.1/32"] !=
				banman.NoCompactFilters {

				t.Fatalf("bans %v, want 127.0.0.1/32 for %v",
					got, banman.NoCompactFilters)
			}
		})
	}
}

// TestRequestMempoolFilterServices checks that the mempool filter, and the
// mempool message that follows a match, are only requested from peers that
// advertise bloom filter support.
func TestRequestMempoolFilterServices(t *testing.T) {
	addr, err := bchutil.NewAddressPubKeyHash(
		make([]byte, 20), &chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatal(err)
	}
	script, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := gcs.BuildGCSFilter(
		builder.DefaultP, builder.DefaultM,
		builder.DeriveKey(&chainhash.Hash{}), [][]byte{script},
	)
	if err != nil {
		t.Fatal(err)
	}
	filterBytes, err := filter.NBytes()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		services wire.ServiceFlag
		mempool  bool
	}{
		{"cf", wire.SFNodeNetwork | wire.SFNodeCF, false},
		{"cf and bloom", wire.SFNodeNetwork | wire.SFNodeCF |
			wire.SFNodeBloom, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, _ := newPeerTestService(t)
			var getCFMempool, mempool int32
			ping := make(chan struct{}, 1)
			sp := connectTestPeer(t, s, test.services,
				peer.MessageListeners{
					OnGetCFMempool: func(p *peer.Peer,
						_ *wire.MsgGetCFMempool) {

						atomic.AddInt32(&getCFMempool, 1)
						p.QueueMessage(wire.NewMsgCFilter(
							wire.GCSFilterRegular,
							&chainhash.Hash{}, filterBytes,
						), nil)
					},
					OnMemPool: func(*peer.Peer, *wire.MsgMemPool) {
						atomic.AddInt32(&mempool, 1)
					},
					OnPing: func(*peer.Peer, *wire.MsgPing) {
						select {
						case ping <- struct{}{}:
						default:
						}
					},
				})

			go func() {
				for {
					select {
					case msg := <-s.query:
						if m, ok := msg.(getPeersMsg); ok {
							m.reply <- []*ServerPeer{sp}
						}
					case <-s.quit:
						return
					}
				}
			}()

			s.requestMempoolFilter([]bchutil.Address{addr})

			// Messages arrive in order, so once the ping is in, so is
			// anything requestMempoolFilter sent.
			sp.QueueMessage(wire.NewMsgPing(1), nil)
			select {
			case <-ping:
			case <-time.After(10 * time.Second):
				t.Fatal("ping timed out")
			}

			wantRequests := int32(0)
			if test.mempool {
				wantRequests = 1
			}
			if n := atomic.LoadInt32(&getCFMempool); n != wantRequests {
				t.Fatalf("got %d getcfmempool messages, want %d",
					n, wantRequests)
			}
			if n := atomic.LoadInt32(&mempool); n != wantRequests {
				t.Fatalf("got %d mempool messages, want %d",
					n, wantRequests)
			}
		})
	}
}

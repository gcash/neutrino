package neutrino

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gcash/bchd/addrmgr"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/connmgr"
	"github.com/gcash/bchd/wire"
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

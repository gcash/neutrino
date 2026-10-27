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

// TestNewAddressAfterFailedDials checks that newAddress picks the peer tried in
// the last 10 minutes, as by the run before a restart, again once its dials to
// an address that can't be reached have failed.
func TestNewAddressAfterFailedDials(t *testing.T) {
	s := newTestService(t)
	good := wire.NewNetAddressV2IPPort(net.ParseIP("35.208.165.58"), 8333,
		RequiredServices)
	unreachable := wire.NewNetAddressV2IPPort(net.ParseIP("148.113.143.138"),
		8333, RequiredServices)
	s.addrManager.AddAddresses([]*wire.NetAddressV2{good, unreachable}, good)
	s.addrManager.Good(good)

	// No dial succeeds, so only newAddress can mark the attempts.
	for i := 0; i < 50; i++ {
		addr, err := s.newAddress()
		if err != nil {
			t.Fatal(err)
		}
		if addr.String() == "35.208.165.58:8333" {
			return
		}
	}
	t.Fatal("newAddress never picked the peer tried in the last 10 minutes")
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

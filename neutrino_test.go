package neutrino

import (
	"net"
	"testing"
	"time"

	"github.com/gcash/bchd/addrmgr"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/neutrino/banman"
)

// noBans is a ban store that has banned no one.
type noBans struct{}

func (noBans) BanIPNet(*net.IPNet, banman.Reason, time.Duration) error { return nil }

func (noBans) Status(*net.IPNet) (banman.Status, error) { return banman.Status{}, nil }

// TestNewAddressAfterFailedDials checks that newAddress picks the peer tried in
// the last 10 minutes, as by the run before a restart, again once its dials to
// an address that can't be reached have failed.
func TestNewAddressAfterFailedDials(t *testing.T) {
	// With quit closed, the service reports no connected peers and no
	// outbound groups without a peer handler.
	quit := make(chan struct{})
	close(quit)
	amgr := addrmgr.New(t.TempDir(), nil)
	s := &ChainService{
		chainParams: chaincfg.MainNetParams,
		addrManager: amgr,
		banStore:    noBans{},
		query:       make(chan interface{}),
		quit:        quit,
		nameResolver: func(host string) ([]net.IP, error) {
			return []net.IP{net.ParseIP(host)}, nil
		},
	}

	good := wire.NewNetAddressV2IPPort(net.ParseIP("35.208.165.58"), 8333,
		RequiredServices)
	unreachable := wire.NewNetAddressV2IPPort(net.ParseIP("148.113.143.138"),
		8333, RequiredServices)
	amgr.AddAddresses([]*wire.NetAddressV2{good, unreachable}, good)
	amgr.Good(good)

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

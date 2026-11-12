package neutrino_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/gcash/bchd/bchec"
	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/integration/rpctest"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchwallet/walletdb"
)

// TestMempoolIntervalFilters checks end to end that a ChainService syncing
// script filters, watching an address, hears of a payment the node admits to
// its mempool afterwards. The peer doesn't announce transactions to this
// service, so the transaction can only have come from an interval filter.
func TestMempoolIntervalFilters(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, nil)
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
	waitForScriptFilterHeaders(t, svc, node)

	privKey, err := bchec.NewPrivateKey(bchec.S256())
	if err != nil {
		t.Fatalf("unable to generate key: %v", err)
	}
	addr, err := bchutil.NewAddressPubKeyHash(
		bchutil.Hash160(privKey.PubKey().SerializeCompressed()),
		&chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatalf("unable to create address: %v", err)
	}
	script, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatalf("unable to create script: %v", err)
	}

	reported := make(chan *bchutil.Tx, 1)
	svc.RegisterMempoolCallback(func(tx *bchutil.Tx, _ *btcjson.BlockDetails) {
		select {
		case reported <- tx:
		default:
		}
	})
	svc.NotifyMempoolReceived([]bchutil.Address{addr})
	svc.RequestMempoolFilter([]bchutil.Address{addr}, nil)

	payment, err := node.CreateTransaction(
		[]*wire.TxOut{{Value: 100000000, PkScript: script}}, 10, true,
	)
	if err != nil {
		t.Fatalf("unable to create payment: %v", err)
	}
	if _, err := node.Node.SendRawTransaction(payment, true); err != nil {
		t.Fatalf("unable to send payment: %v", err)
	}

	// The peer doesn't announce transactions to this service, so the
	// payment can only arrive in an interval's filter.
	select {
	case tx := <-reported:
		if *tx.Hash() != payment.TxHash() {
			t.Fatalf("reported %v, want the payment %v", tx.Hash(),
				payment.TxHash())
		}
	case <-time.After(time.Minute):
		t.Fatal("the payment wasn't reported from a mempool interval " +
			"within one minute")
	}
}

// TestMempoolScriptFilterRequest checks end to end that a ChainService syncing
// script filters, started while a payment to an address it watches waits in a
// node's mempool, asks the node for the script filter of that mempool and
// reports the payment.
func TestMempoolScriptFilterRequest(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, nil)
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	defer func() { _ = node.TearDown() }()
	if err := node.SetUp(true, 10); err != nil {
		t.Fatalf("unable to set up harness: %v", err)
	}

	privKey, err := bchec.NewPrivateKey(bchec.S256())
	if err != nil {
		t.Fatalf("unable to generate key: %v", err)
	}
	addr, err := bchutil.NewAddressPubKeyHash(
		bchutil.Hash160(privKey.PubKey().SerializeCompressed()),
		&chaincfg.SimNetParams,
	)
	if err != nil {
		t.Fatalf("unable to create address: %v", err)
	}
	script, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatalf("unable to create script: %v", err)
	}
	payment, err := node.CreateTransaction(
		[]*wire.TxOut{{Value: 100000000, PkScript: script}}, 10, true,
	)
	if err != nil {
		t.Fatalf("unable to create payment: %v", err)
	}
	if _, err := node.Node.SendRawTransaction(payment, true); err != nil {
		t.Fatalf("unable to send payment: %v", err)
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
	waitForScriptFilterHeaders(t, svc, node)

	reported := make(chan *bchutil.Tx, 1)
	svc.RegisterMempoolCallback(func(tx *bchutil.Tx, _ *btcjson.BlockDetails) {
		select {
		case reported <- tx:
		default:
		}
	})
	svc.NotifyMempoolReceived([]bchutil.Address{addr})
	svc.RequestMempoolFilter([]bchutil.Address{addr}, nil)

	select {
	case tx := <-reported:
		if *tx.Hash() != payment.TxHash() {
			t.Fatalf("reported %v, want the payment %v", tx.Hash(),
				payment.TxHash())
		}
	case <-time.After(time.Minute):
		t.Fatal("the payment in the node's mempool wasn't reported " +
			"from its script filter within one minute")
	}
}

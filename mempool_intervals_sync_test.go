package neutrino

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/gcash/bchd/bchec"
	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/integration/rpctest"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchwallet/walletdb"
	_ "github.com/gcash/bchwallet/walletdb/bdb"
)

// intervalHeaderWait is how long these tests wait for script filter headers.
// The other end-to-end script filter tests use the same bound.
const intervalHeaderWait = 3 * time.Minute

// newSimnet returns a harness of a simnet node with 10 blocks.
func newSimnet(t *testing.T) *rpctest.Harness {
	t.Helper()
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, nil)
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	if err := node.SetUp(true, 10); err != nil {
		_ = node.TearDown()
		t.Fatalf("unable to set up harness: %v", err)
	}
	return node
}

// startIntervalService starts a ChainService that syncs script filters and
// connects only to node. floor is MempoolFetchFloor; zero is the default.
func startIntervalService(t *testing.T, node *rpctest.Harness, floor uint32) (*ChainService, func()) {
	t.Helper()
	dataDir := t.TempDir()
	db, err := walletdb.Create("bdb", filepath.Join(dataDir, "neutrino.db"), true)
	if err != nil {
		t.Fatalf("unable to create db: %v", err)
	}
	svc, err := NewChainService(Config{
		DataDir:           dataDir,
		Database:          db,
		ChainParams:       chaincfg.SimNetParams,
		ConnectPeers:      []string{node.P2PAddress()},
		FilterType:        wire.GCSFilterScripts,
		MempoolFetchFloor: floor,
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("unable to create ChainService: %v", err)
	}
	if err := svc.Start(); err != nil {
		_ = db.Close()
		t.Fatalf("unable to start ChainService: %v", err)
	}
	return svc, func() {
		_ = svc.Stop()
		_ = db.Close()
	}
}

// waitForIntervalHeaders waits until svc has the node's block headers and
// script filter headers.
func waitForIntervalHeaders(t *testing.T, svc *ChainService, node *rpctest.Harness) {
	t.Helper()
	bestHash, _, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatalf("unable to get best block: %v", err)
	}
	header, err := node.Node.GetCFilterHeader(bestHash, wire.GCSFilterScripts)
	if err != nil {
		t.Fatalf("unable to get the script filter header of %v: %v", bestHash, err)
	}
	want := header.PrevFilterHeader

	deadline := time.Now().Add(intervalHeaderWait)
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

// newP2PKH returns a fresh address and the script that pays it.
func newP2PKH(t *testing.T) (bchutil.Address, []byte) {
	t.Helper()
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
	return addr, script
}

// sendToNode admits a transaction paying script to the node's mempool.
func sendToNode(t *testing.T, node *rpctest.Harness, script []byte, value int64) *wire.MsgTx {
	t.Helper()
	tx, err := node.CreateTransaction(
		[]*wire.TxOut{{Value: value, PkScript: script}}, 10, true,
	)
	if err != nil {
		t.Fatalf("unable to create transaction: %v", err)
	}
	if _, err := node.Node.SendRawTransaction(tx, true); err != nil {
		t.Fatalf("unable to send transaction: %v", err)
	}
	return tx
}

// waitForMempoolTx waits until hash is in svc's mempool. The node's interval
// closes on its own timer, so this is a bound on that, not a sleep of a
// fixed interval.
func waitForMempoolTx(t *testing.T, svc *ChainService, hash chainhash.Hash, what string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		if svc.mempool.HaveTransaction(&hash) {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("%s %v was not in the mempool within one minute", what, hash)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestMempoolIntervalFilters checks end to end that a ChainService syncing
// script filters, watching an address, hears of a payment the node admits to
// its mempool afterwards. The peer doesn't announce transactions to this
// service, so the payment can only have come from an interval filter. A
// transaction that pays nothing the service watches is also downloaded,
// because an interval smaller than the default floor is taken whatever it holds.
func TestMempoolIntervalFilters(t *testing.T) {
	node := newSimnet(t)
	defer func() { _ = node.TearDown() }()

	svc, cleanup := startIntervalService(t, node, 0)
	defer cleanup()
	waitForIntervalHeaders(t, svc, node)

	addr, script := newP2PKH(t)
	_, otherScript := newP2PKH(t)
	reported := make(chan *bchutil.Tx, 1)
	svc.RegisterMempoolCallback(func(tx *bchutil.Tx, _ *btcjson.BlockDetails) {
		select {
		case reported <- tx:
		default:
		}
	})
	svc.NotifyMempoolReceived([]bchutil.Address{addr})
	svc.RequestMempoolFilter([]bchutil.Address{addr}, nil)

	// The watched script is not in the mempool yet, so the filter of the
	// whole mempool does not match. The unrelated transaction can only
	// arrive by its interval being downloaded.
	unrelated := sendToNode(t, node, otherScript, 50000000)
	waitForMempoolTx(t, svc, unrelated.TxHash(),
		"a transaction that pays nothing watched")

	payment := sendToNode(t, node, script, 100000000)
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

// TestMempoolFetchFloorRequiresMatch checks that a floor of one downloads an
// interval only when its filter matches. The payment to the watched address
// is reported. The unrelated transaction was admitted in an earlier interval:
// a second service, on the default floor, has it in its mempool only once
// that interval has closed and been downloaded, and the payment is sent
// after that. The floor of one then does not have the unrelated transaction.
func TestMempoolFetchFloorRequiresMatch(t *testing.T) {
	node := newSimnet(t)
	defer func() { _ = node.TearDown() }()

	companion, cleanupCompanion := startIntervalService(t, node, 0)
	defer cleanupCompanion()
	floor, cleanupFloor := startIntervalService(t, node, 1)
	defer cleanupFloor()
	waitForIntervalHeaders(t, companion, node)
	waitForIntervalHeaders(t, floor, node)

	addr, script := newP2PKH(t)
	_, otherScript := newP2PKH(t)
	reported := make(chan *bchutil.Tx, 1)
	floor.RegisterMempoolCallback(func(tx *bchutil.Tx, _ *btcjson.BlockDetails) {
		select {
		case reported <- tx:
		default:
		}
	})
	floor.NotifyMempoolReceived([]bchutil.Address{addr})
	floor.RequestMempoolFilter([]bchutil.Address{addr}, nil)

	unrelated := sendToNode(t, node, otherScript, 50000000)
	waitForMempoolTx(t, companion, unrelated.TxHash(),
		"the unrelated transaction on the service that downloads every small interval")

	payment := sendToNode(t, node, script, 100000000)
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
	sum := unrelated.TxHash()
	if floor.mempool.HaveTransaction(&sum) {
		t.Fatalf("unrelated transaction %v was downloaded with floor 1", sum)
	}
}

// TestMempoolScriptFilterRequest checks end to end that a ChainService syncing
// script filters, started while a payment to an address it watches waits in a
// node's mempool, asks the node for the script filter of that mempool and
// reports the payment.
func TestMempoolScriptFilterRequest(t *testing.T) {
	node := newSimnet(t)
	defer func() { _ = node.TearDown() }()

	addr, script := newP2PKH(t)
	payment := sendToNode(t, node, script, 100000000)

	svc, cleanup := startIntervalService(t, node, 0)
	defer cleanup()
	waitForIntervalHeaders(t, svc, node)

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

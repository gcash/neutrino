package neutrino_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/integration/rpctest"
	"github.com/gcash/bchd/rpcclient"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchwallet/waddrmgr"
	"github.com/gcash/bchwallet/walletdb"
	"github.com/gcash/neutrino"
)

// TestScriptFilterRescan checks end to end that, on a ChainService that syncs
// script filters, a rescan that watches only an address reports the payment
// to it and the later spend from it, whose block the script filter matches by
// the script of the address; that the output and spend searches find the same
// two transactions by script, while a spend search by outpoints alone is
// refused; and that the UTXO scanner reports the spend.
func TestScriptFilterRescan(t *testing.T) {
	node, err := rpctest.New(&chaincfg.SimNetParams, nil, []string{"--txindex"})
	if err != nil {
		t.Fatalf("unable to create harness: %v", err)
	}
	defer func() { _ = node.TearDown() }()
	if err := node.SetUp(true, 10); err != nil {
		t.Fatalf("unable to set up harness: %v", err)
	}

	// An address anyone can spend from, by showing its script.
	redeem := []byte{txscript.OP_TRUE}
	addr, err := bchutil.NewAddressScriptHash(redeem, &chaincfg.SimNetParams)
	if err != nil {
		t.Fatal(err)
	}
	script, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatal(err)
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
	if _, err := node.Node.Generate(1); err != nil {
		t.Fatalf("unable to mine: %v", err)
	}
	_, paymentHeight, err := node.Node.GetBestBlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := node.Node.Generate(3); err != nil {
		t.Fatalf("unable to mine: %v", err)
	}

	// Spend it to another script.
	paymentHash := payment.TxHash()
	var paid uint32
	for i, txOut := range payment.TxOut {
		if bytes.Equal(txOut.PkScript, script) {
			paid = uint32(i)
		}
	}
	outPoint := wire.NewOutPoint(&paymentHash, paid)
	spend := wire.NewMsgTx(2)
	spend.AddTxIn(wire.NewTxIn(outPoint, []byte{txscript.OP_DATA_1, txscript.OP_TRUE}))
	spend.AddTxOut(wire.NewTxOut(99990000, []byte{
		txscript.OP_DUP, txscript.OP_HASH160, txscript.OP_DATA_20,
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		txscript.OP_EQUALVERIFY, txscript.OP_CHECKSIG,
	}, wire.TokenData{}))
	if _, err := node.Node.SendRawTransaction(spend, true); err != nil {
		t.Fatalf("unable to send spend: %v", err)
	}
	if _, err := node.Node.Generate(1); err != nil {
		t.Fatalf("unable to mine: %v", err)
	}
	_, spendHeight, err := node.Node.GetBestBlock()
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

	// The rescan watches the address alone, from the genesis block.
	var (
		mtx     sync.Mutex
		found   = make(map[int32][]chainhash.Hash)
		reached int32
	)
	quit := make(chan struct{})
	genesis := *chaincfg.SimNetParams.GenesisHash
	rescan := neutrino.NewRescan(
		&neutrino.RescanChainSource{ChainService: svc},
		neutrino.QuitChan(quit),
		neutrino.WatchAddrs(addr),
		neutrino.StartBlock(&waddrmgr.BlockStamp{Hash: genesis}),
		neutrino.NotificationHandlers(rpcclient.NotificationHandlers{
			OnFilteredBlockConnected: func(height int32,
				_ *wire.BlockHeader, relevantTxs []*bchutil.Tx) {

				mtx.Lock()
				defer mtx.Unlock()
				for _, tx := range relevantTxs {
					found[height] = append(found[height], *tx.Hash())
				}
				reached = height
			},
		}),
	)
	errChan := rescan.Start()
	deadline := time.Now().Add(repairTimeout)
	for {
		mtx.Lock()
		done := reached >= tip
		mtx.Unlock()
		if done {
			break
		}
		select {
		case err := <-errChan:
			t.Fatalf("the rescan ended: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the rescan to reach height %d", tip)
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(quit)

	mtx.Lock()
	want := map[int32][]chainhash.Hash{
		paymentHeight: {paymentHash},
		spendHeight:   {spend.TxHash()},
	}
	if !reflect.DeepEqual(found, want) {
		t.Errorf("the rescan reported %v, want %v", found, want)
	}
	mtx.Unlock()

	// The searches find the same two transactions by the script.
	var outputs []neutrino.FoundOutput
	if _, err := svc.SearchOutputs([][]byte{script}, 1, tip,
		func(output neutrino.FoundOutput) bool {
			outputs = append(outputs, output)
			return true
		}, nil, nil); err != nil {

		t.Fatalf("output search: %v", err)
	}
	if len(outputs) != 1 || outputs[0].Height != paymentHeight {
		t.Errorf("output search found %+v, want the payment at height %d",
			outputs, paymentHeight)
	}
	var spends []neutrino.FoundSpend
	inputs := []neutrino.InputWithScript{{OutPoint: *outPoint, PkScript: script}}
	if _, err := svc.SearchScriptSpends(inputs, 1, tip,
		func(found neutrino.FoundSpend) []neutrino.InputWithScript {
			spends = append(spends, found)
			return nil
		}, nil, nil); err != nil {

		t.Fatalf("spend search: %v", err)
	}
	if len(spends) != 1 || spends[0].Height != spendHeight ||
		spends[0].SpendingTx.TxHash() != spend.TxHash() {

		t.Errorf("spend search found %+v, want the spend at height %d",
			spends, spendHeight)
	}
	_, err = svc.SearchSpends([]wire.OutPoint{*outPoint}, 1, tip, nil, nil, nil)
	if !errors.Is(err, neutrino.ErrSpendSearchNeedsScripts) {
		t.Errorf("a spend search by outpoints alone: %v", err)
	}

	// The UTXO scanner reports the spend too, from the script it is given
	// with the outpoint.
	report, err := svc.GetUtxo(
		neutrino.WatchInputs(inputs[0]),
		neutrino.StartBlock(&waddrmgr.BlockStamp{Height: paymentHeight}),
	)
	if err != nil {
		t.Fatalf("unable to get the UTXO: %v", err)
	}
	if report == nil || report.SpendingTx == nil ||
		report.SpendingTx.TxHash() != spend.TxHash() {

		t.Errorf("the UTXO scanner reported %+v, want the spend", report)
	}
}

// TestCategoriesNeedScriptFilters checks that a rescan given a token category
// to watch on regular filters, which hold none, ends with
// ErrCategoriesNeedScriptFilters.
func TestCategoriesNeedScriptFilters(t *testing.T) {
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
	svc := startChainService(t, dataDir, db, node)
	defer func() { _ = svc.Stop() }()

	quit := make(chan struct{})
	defer close(quit)
	rescan := neutrino.NewRescan(
		&neutrino.RescanChainSource{ChainService: svc},
		neutrino.QuitChan(quit),
		neutrino.WatchCategories([32]byte{0xcc}),
		neutrino.NotificationHandlers(rpcclient.NotificationHandlers{}),
	)
	select {
	case err := <-rescan.Start():
		if !errors.Is(err, neutrino.ErrCategoriesNeedScriptFilters) {
			t.Errorf("the rescan ended with %v", err)
		}
	case <-time.After(time.Minute):
		t.Error("the rescan was not refused")
	}
}

package neutrino

import (
	"bytes"
	"testing"

	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchwallet/waddrmgr"
)

// blockSource is a ChainSource that serves a single block.
type blockSource struct {
	ChainSource
	block *wire.MsgBlock
}

func (s *blockSource) GetBlock(chainhash.Hash, ...QueryOption) (
	*bchutil.Block, error) {

	return bchutil.NewBlock(s.block), nil
}

// payTo returns the output script that pays to a P2PKH address made of fill.
func payTo(t *testing.T, fill byte) (bchutil.Address, []byte) {
	t.Helper()

	addr, err := bchutil.NewAddressPubKeyHash(
		bytes.Repeat([]byte{fill}, 20), &chaincfg.RegressionNetParams,
	)
	if err != nil {
		t.Fatal(err)
	}
	script, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatal(err)
	}
	return addr, script
}

// TestExtractBlockMatchesSpendBeforeParent checks that a transaction that
// spends an output paying a watched address is matched when the block lists it
// before the transaction that creates the output, as blocks that order their
// transactions by txid do.
func TestExtractBlockMatchesSpendBeforeParent(t *testing.T) {
	watched, watchedScript := payTo(t, 1)
	_, otherScript := payTo(t, 2)

	parent := wire.NewMsgTx(wire.TxVersion)
	parent.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash{1}}, nil))
	parent.AddTxOut(&wire.TxOut{Value: 1000, PkScript: watchedScript})

	// The spend pays no watched address, so only its input makes it
	// relevant.
	spend := wire.NewMsgTx(wire.TxVersion)
	spend.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: parent.TxHash()}, nil))
	spend.AddTxOut(&wire.TxOut{Value: 900, PkScript: otherScript})

	unrelated := wire.NewMsgTx(wire.TxVersion)
	unrelated.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash{2}}, nil))
	unrelated.AddTxOut(&wire.TxOut{Value: 800, PkScript: otherScript})

	chain := &blockSource{block: &wire.MsgBlock{
		Transactions: []*wire.MsgTx{spend, unrelated, parent},
	}}
	ro := &rescanOptions{watchAddrs: []bchutil.Address{watched}}
	txs, err := extractBlockMatches(chain, ro, &waddrmgr.BlockStamp{Height: 1})
	if err != nil {
		t.Fatal(err)
	}

	want := []chainhash.Hash{spend.TxHash(), parent.TxHash()}
	var got []chainhash.Hash
	for _, tx := range txs {
		got = append(got, *tx.Hash())
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("matched %v, want the spend and its parent %v", got, want)
	}
}

// TestMatchesTokenOutputs checks that a transaction that pays a watched
// address with an output carrying CashTokens is found, in a block and in the
// mempool. It goes through the wire encoding, which carries the token prefix in
// the output's script, as from a peer.
func TestMatchesTokenOutputs(t *testing.T) {
	watched, watchedScript := payTo(t, 1)
	tokens := wire.TokenData{CategoryID: [32]byte{1}, Amount: 1000,
		BitField: wire.HAS_AMOUNT}

	sent := wire.NewMsgTx(2)
	sent.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash{1}}, nil))
	sent.AddTxOut(wire.NewTxOut(1000, watchedScript, tokens))
	var buf bytes.Buffer
	if err := sent.Serialize(&buf); err != nil {
		t.Fatal(err)
	}
	var tx wire.MsgTx
	if err := tx.Deserialize(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if tx.TxOut[0].TokenData.IsEmpty() {
		t.Fatal("the decoded output carries no tokens")
	}

	chain := &blockSource{block: &wire.MsgBlock{
		Transactions: []*wire.MsgTx{&tx},
	}}
	ro := &rescanOptions{watchAddrs: []bchutil.Address{watched}}
	txs, err := extractBlockMatches(chain, ro, &waddrmgr.BlockStamp{Height: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(txs) != 1 {
		t.Errorf("matched %d transactions in the block, want the one "+
			"paying tokens", len(txs))
	}

	mempool := NewMempool()
	mempool.NotifyReceived([]bchutil.Address{watched})
	var reported int
	mempool.RegisterCallback(func(*bchutil.Tx, *btcjson.BlockDetails) {
		reported++
	})
	mempool.AddTransaction(bchutil.NewTx(&tx))
	if reported != 1 {
		t.Errorf("the mempool reported %d transactions, want the one "+
			"paying tokens", reported)
	}
}

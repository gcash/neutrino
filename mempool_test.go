package neutrino

import (
	"bytes"
	"testing"

	"github.com/gcash/bchd/bchec"
	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
)

// TestMempoolReportsSpends checks that the mempool reports a transaction that
// spends an output paying a watched address, as another wallet with the same
// keys sends, telling it by the input's signature script, and not one whose
// inputs spend other outputs.
func TestMempoolReportsSpends(t *testing.T) {
	params := &chaincfg.RegressionNetParams
	pubKey := func() []byte {
		key, err := bchec.NewPrivateKey(bchec.S256())
		if err != nil {
			t.Fatal(err)
		}
		return key.PubKey().SerializeCompressed()
	}
	watchedKey, otherKey := pubKey(), pubKey()
	redeem := []byte{txscript.OP_1}
	redeem32 := []byte{txscript.OP_2}

	pkh, err := bchutil.NewAddressPubKeyHash(bchutil.Hash160(watchedKey),
		params)
	if err != nil {
		t.Fatal(err)
	}
	p2sh, err := bchutil.NewAddressScriptHash(redeem, params)
	if err != nil {
		t.Fatal(err)
	}
	p2sh32, err := bchutil.NewAddressScriptHash32(redeem32, params)
	if err != nil {
		t.Fatal(err)
	}
	_, otherScript := payTo(t, 2)

	pushes := func(data ...[]byte) []byte {
		builder := txscript.NewScriptBuilder()
		for _, d := range data {
			builder.AddData(d)
		}
		script, err := builder.Script()
		if err != nil {
			t.Fatal(err)
		}
		return script
	}
	sig := bytes.Repeat([]byte{0x30}, 71)

	tests := []struct {
		name      string
		sigScript []byte
		reported  bool
	}{
		{"P2PKH", pushes(sig, watchedKey), true},
		{"P2SH", pushes(nil, sig, redeem), true},
		{"P2SH32", pushes(sig, redeem32), true},
		{"another key", pushes(sig, otherKey), false},
		{"key not last", pushes(watchedKey, sig), false},
		{"malformed", []byte{txscript.OP_DATA_33, 1, 2}, false},
		{"empty", nil, false},
	}
	for i, test := range tests {
		mempool := NewMempool()
		mempool.NotifyReceived([]bchutil.Address{pkh, p2sh, p2sh32})
		var reported bool
		mempool.RegisterCallback(func(*bchutil.Tx, *btcjson.BlockDetails) {
			reported = true
		})

		tx := wire.NewMsgTx(2)
		tx.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash{byte(i)}},
			test.sigScript))
		tx.AddTxOut(wire.NewTxOut(1000, otherScript, wire.TokenData{}))
		mempool.AddTransaction(bchutil.NewTx(tx))
		if reported != test.reported {
			t.Errorf("%s: reported %v, want %v", test.name, reported,
				test.reported)
		}
	}
}

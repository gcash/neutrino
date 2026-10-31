package neutrino

import (
	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchutil"
	"sync"
)

// Mempool is used when we are downloading unconfirmed transactions.
// We will use this object to track which transactions we've already
// downloaded so that we don't download them more than once.
type Mempool struct {
	downloadedTxs map[chainhash.Hash]bool
	mtx           sync.RWMutex
	callbacks     []func(tx *bchutil.Tx, block *btcjson.BlockDetails)
	watchedAddrs  []bchutil.Address

	// watchedScripts holds the output scripts that pay the watched
	// addresses, to tell the inputs that spend such outputs.
	watchedScripts map[string]struct{}
}

// NewMempool returns an initialized mempool
func NewMempool() *Mempool {
	return &Mempool{
		downloadedTxs:  make(map[chainhash.Hash]bool),
		mtx:            sync.RWMutex{},
		watchedScripts: make(map[string]struct{}),
	}
}

// RegisterCallback will register a callback that will fire when a transaction
// matching a watched address enters the mempool.
func (mp *Mempool) RegisterCallback(onRecvTx func(tx *bchutil.Tx, block *btcjson.BlockDetails)) {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	mp.callbacks = append(mp.callbacks, onRecvTx)
}

// HaveTransaction returns whether or not the passed transaction already exists
// in the mempool.
func (mp *Mempool) HaveTransaction(hash *chainhash.Hash) bool {
	mp.mtx.RLock()
	defer mp.mtx.RUnlock()
	return mp.downloadedTxs[*hash]
}

// AddTransaction adds a new transaction to the mempool and calls back if it
// pays a watched address or spends an output that pays one.
func (mp *Mempool) AddTransaction(tx *bchutil.Tx) {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	mp.downloadedTxs[*tx.Hash()] = true

	ro := defaultRescanOptions()
	WatchAddrs(mp.watchedAddrs...)(ro)
	pays, err := ro.paysWatchedAddr(tx)
	if (pays && err == nil) || mp.spendsWatchedAddr(tx) {
		for _, cb := range mp.callbacks {
			cb(tx, nil)
		}
	}
}

// spendsWatchedAddr returns whether an input of tx spends an output that pays
// a watched address.  The mempool doesn't have the outputs that tx spends, so
// it tells them by the inputs' signature scripts, as BIP37 filters do: the last
// push of an input is the public key whose hash the P2PKH output it spends
// pays, or the redeem script whose hash the P2SH output it spends pays.
func (mp *Mempool) spendsWatchedAddr(tx *bchutil.Tx) bool {
	for _, txIn := range tx.MsgTx().TxIn {
		pushes, err := txscript.PushedData(txIn.SignatureScript)
		if err != nil || len(pushes) == 0 {
			continue
		}
		for _, script := range spentScripts(pushes[len(pushes)-1]) {
			if _, ok := mp.watchedScripts[string(script)]; ok {
				return true
			}
		}
	}
	return false
}

// spentScripts returns the output scripts that an input whose signature script
// pushes last could spend: a P2PKH script if last is a public key, and the
// P2SH and P2SH32 scripts if it's a redeem script.
func spentScripts(last []byte) [][]byte {
	hash := bchutil.Hash160(last)
	p2pkh, _ := txscript.NewScriptBuilder().AddOp(txscript.OP_DUP).
		AddOp(txscript.OP_HASH160).AddData(hash).
		AddOp(txscript.OP_EQUALVERIFY).AddOp(txscript.OP_CHECKSIG).Script()
	p2sh, _ := txscript.NewScriptBuilder().AddOp(txscript.OP_HASH160).
		AddData(hash).AddOp(txscript.OP_EQUAL).Script()
	p2sh32, _ := txscript.NewScriptBuilder().AddOp(txscript.OP_HASH256).
		AddData(bchutil.Hash256(last)).AddOp(txscript.OP_EQUAL).Script()
	return [][]byte{p2pkh, p2sh, p2sh32}
}

// Clear will remove all transactions from the mempool. This
// should be done whenever a new block is accepted.
func (mp *Mempool) Clear() {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	mp.downloadedTxs = make(map[chainhash.Hash]bool)
}

// NotifyReceived stores addresses to watch
func (mp *Mempool) NotifyReceived(addrs []bchutil.Address) {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	mp.watchedAddrs = append(mp.watchedAddrs, addrs...)
	for _, addr := range addrs {
		script, err := txscript.PayToAddrScript(addr)
		if err != nil {
			continue
		}
		mp.watchedScripts[string(script)] = struct{}{}
	}
}

package neutrino

import (
	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg/chainhash"
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
}

// NewMempool returns an initialized mempool
func NewMempool() *Mempool {
	return &Mempool{
		downloadedTxs: make(map[chainhash.Hash]bool),
		mtx:           sync.RWMutex{},
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

// AddTransaction adds a new transaction to the mempool and
// maybe calls back if it matches any watched addresses.
func (mp *Mempool) AddTransaction(tx *bchutil.Tx) {
	mp.mtx.Lock()
	defer mp.mtx.Unlock()
	mp.downloadedTxs[*tx.Hash()] = true

	ro := defaultRescanOptions()
	WatchAddrs(mp.watchedAddrs...)(ro)
	if ok, err := ro.paysWatchedAddr(tx); ok && err == nil {
		for _, cb := range mp.callbacks {
			cb(tx, nil)
		}
	}
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
}

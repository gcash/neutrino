package neutrino

import (
	"errors"
	"sync"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// A node gathers the transactions it admits to its mempool into intervals of
// a few seconds and builds one script filter for each, keyed by the hash of
// those transactions.  A peer we asked with sendmpfilter sends an mpfilter as
// each interval closes.  We match it here with the scripts we watch and, on a
// match, ask for the transactions of a run of intervals with getmptxs.  The
// run holds at least mpRunMinTxs transactions when the peer has sent filters
// for that many, so the request does not tell the peer which transaction we
// want.  The node answers with one mptxs for each interval in the run, and
// the transactions still in its mempool are handed to ours.
//
// This is how a service that syncs script filters hears of unconfirmed
// transactions.  One that syncs regular filters keeps asking for the filter
// of the whole mempool, and takes the transactions peers announce.

// mpRunMinTxs is the fewest transactions a run of intervals that we ask a
// peer for holds, when the peer has sent us filters for that many: with
// fewer, asking would tell the peer which transaction we want.
var mpRunMinTxs uint32 = 1000

const (
	// mpIntervalsKept is an hour of intervals, which is what a node keeps.
	mpIntervalsKept = 720

	// mpTxsBanScore is what bchd adds to a peer's ban score for a getmptxs.
	mpTxsBanScore = 5
)

// errMpTxs is returned for an mptxs whose transactions are not the ones the
// peer announced for that interval.
var errMpTxs = errors.New("mptxs does not match the interval the peer announced")

// mpInterval is one interval of mempool transactions a peer has announced.
type mpInterval struct {
	hash    chainhash.Hash
	txCount uint32

	// asked is set once the interval was included in a getmptxs, until the
	// mptxs for it arrives.
	asked bool
}

// mpPeer is the intervals one peer has sent.  order is their sequence numbers
// as they arrived, so the oldest can be dropped.
type mpPeer struct {
	intervals map[uint32]*mpInterval
	order     []uint32
}

// mempoolIntervals matches the script filters of mempool intervals with the
// scripts we watch, asks for a run of intervals when one matches, and
// delivers the transactions of a run we asked for.
type mempoolIntervals struct {
	mtx sync.Mutex

	// entries are the script keys we watch, and deliver takes a
	// transaction of an interval we asked for.
	entries func() [][]byte
	deliver func(tx *bchutil.Tx)

	// peers are the intervals each peer has sent, by the peer's address.
	peers map[string]*mpPeer
}

// newMempoolIntervals returns an interval watcher.  entries returns the
// script keys to match filters against, and deliver takes each full
// transaction of a run we asked for.
func newMempoolIntervals(entries func() [][]byte, deliver func(tx *bchutil.Tx)) *mempoolIntervals {
	return &mempoolIntervals{
		entries: entries,
		deliver: deliver,
		peers:   make(map[string]*mpPeer),
	}
}

// onFilter records the interval msg announces for peer.  When its filter
// matches a script we watch and canAsk is set, it returns a getmptxs for a
// run of intervals that holds it.  The run reaches back over the contiguous
// intervals the peer sent just before this one until it holds mpRunMinTxs
// transactions.  It returns nil when the message is not a script filter,
// nothing is watched, the filter matches nothing, or canAsk is false; a
// script filter is recorded in every one of those cases.
func (m *mempoolIntervals) onFilter(peer string, msg *wire.MsgMpFilter, canAsk bool) *wire.MsgGetMpTxs {
	if msg.FilterType != wire.GCSFilterScripts {
		return nil
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	p := m.peers[peer]
	if p == nil {
		p = &mpPeer{intervals: make(map[uint32]*mpInterval)}
		m.peers[peer] = p
	}
	// A sequence number sent again is the same interval announced anew, so
	// it keeps its place and the old announcement is dropped.
	_, known := p.intervals[msg.Seq]
	p.intervals[msg.Seq] = &mpInterval{
		hash:    msg.IntervalHash,
		txCount: msg.TxCount,
	}
	if !known {
		p.order = append(p.order, msg.Seq)
		for len(p.order) > mpIntervalsKept {
			delete(p.intervals, p.order[0])
			p.order = p.order[1:]
		}
	}

	// entries only reads the requester, which never calls back here, so
	// holding this lock across it cannot deadlock.
	entries := m.entries()
	if len(entries) == 0 {
		return nil
	}
	filter, err := gcs.FromNBytes(builder.DefaultP, builder.DefaultM, msg.Data)
	if err != nil || filter.N() == 0 {
		return nil
	}
	matched, err := filter.MatchAny(builder.DeriveKey(&msg.IntervalHash), entries)
	if err != nil || !matched {
		return nil
	}
	// The interval stays recorded and not asked for.  A later match can
	// still take it into a run.
	if !canAsk {
		return nil
	}

	// Reach back while the preceding interval is one the peer sent, until
	// the run holds enough transactions that it does not point at the one
	// that matched.  A gap, or the peer having sent fewer, ends the run.
	first, last := msg.Seq, msg.Seq
	total := msg.TxCount
	for total < mpRunMinTxs && first > 0 {
		prev := p.intervals[first-1]
		if prev == nil {
			break
		}
		first--
		total += prev.txCount
	}
	for seq := first; ; seq++ {
		p.intervals[seq].asked = true
		if seq == last {
			break
		}
	}
	return wire.NewMsgGetMpTxs(wire.GCSFilterScripts, first, last)
}

// onTxs checks the transactions peer sent for one interval against the
// interval it announced.  It returns errMpTxs when they differ.  The full
// transactions of an interval we asked for are handed to deliver, without
// the lock held, and an mptxs we did not ask for is ignored.
func (m *mempoolIntervals) onTxs(peer string, msg *wire.MsgMpTxs) error {
	m.mtx.Lock()

	var iv *mpInterval
	if p := m.peers[peer]; p != nil {
		iv = p.intervals[msg.Seq]
	}
	if iv == nil || !iv.asked {
		m.mtx.Unlock()
		return nil
	}
	ids := make([]chainhash.Hash, len(msg.Txs))
	for i := range msg.Txs {
		ids[i] = msg.Txs[i].TxID
	}
	if uint32(len(msg.Txs)) != iv.txCount || wire.MpIntervalHash(ids) != iv.hash {
		m.mtx.Unlock()
		return errMpTxs
	}
	iv.asked = false
	var txs []*wire.MsgTx
	for i := range msg.Txs {
		if msg.Txs[i].Tx != nil {
			txs = append(txs, msg.Txs[i].Tx)
		}
	}
	m.mtx.Unlock()

	for _, tx := range txs {
		m.deliver(bchutil.NewTx(tx))
	}
	return nil
}

// forget drops what we recorded of peer, as when it disconnects.
func (m *mempoolIntervals) forget(peer string) {
	m.mtx.Lock()
	delete(m.peers, peer)
	m.mtx.Unlock()
}

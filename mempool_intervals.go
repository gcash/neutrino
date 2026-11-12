package neutrino

import (
	"errors"
	"sync"
	"time"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// A node gathers the transactions it admits to its mempool into intervals of
// a few seconds and builds one script filter for each, keyed by the hash of
// those transactions.  A service that syncs script filters hears of
// unconfirmed transactions from one feed peer.  Only the feed is asked to
// send the filters, and a filter or a batch of transactions from any other
// peer is ignored.  When the feed disconnects, what it sent is dropped and
// another peer is asked, so two connections never share one record.
//
// An interval with fewer transactions than the floor is always downloaded,
// and its filter is not read.  A wallet on regular filters downloads every
// transaction a peer announces, so for these intervals the requests a peer
// sees are the same whatever the wallet watches.  An interval with that many
// transactions or more is downloaded only when its filter matches a watched
// script, which tells the peer that one of at least that many transactions
// is of interest and nothing more.  Asking only for the interval that
// matched, at the moment it matched, would tell the peer which interval it
// was however the request was padded, which is why a short interval is taken
// whole instead of being hidden inside a run.
//
// An interval to download is pending until the ban score has room for a
// getmptxs.  One request asks for the oldest pending sequence and every
// pending sequence that follows it with no gap.  Those become asked.  An
// asked interval whose transactions have not arrived within mpTxsWait is
// pending again: the node always sends the first interval of a run and may
// stop once it has sent 32MB, so a missing tail is reached by asking again,
// when it is first.  Pending intervals are requested again on each later
// filter and once a second, so a match is fetched once the score has decayed
// even when no later interval arrives.
//
// An answer is accepted when it was asked for, it has the announced number
// of transactions, and the double SHA-256 of their ids is the announced
// interval hash.  It is then done, and a later answer is ignored.  The full
// transactions are delivered without the lock held.  An announcement of a
// sequence again with the same hash and count keeps the interval's state.  A
// different hash or count, or an answer that does not match, means the peer
// is misbehaving.  At most mpIntervalsKept intervals are remembered, which is
// as many as a node keeps; the oldest is dropped whatever its state.

const (
	// DefaultMempoolFetchFloor is the floor used when none is configured.
	// An interval with fewer transactions than the floor is downloaded whole.
	DefaultMempoolFetchFloor = 200

	// mpIntervalsKept is an hour of intervals, which is what a node keeps.
	mpIntervalsKept = 720

	// mpTxsBanScore is what bchd adds to a peer's ban score for a getmptxs.
	mpTxsBanScore = 2

	// mpTxsWait is how long an asked interval stays asked before it is asked
	// again.  The node stops a run once the transactions it has sent pass
	// 32MB, so the tail can be missing, and a message can be lost.
	mpTxsWait = 30 * time.Second

	// mpIntervalTick is how often pending intervals are asked for again when
	// no new filter arrives to ask on.
	mpIntervalTick = time.Second

	// noFeed is the peer id that means there is no feed.  A peer id is taken
	// from a counter that starts at zero and is increased by one when the
	// peer's version message arrives, so the first peer is 1 and no peer is
	// ever 0.
	noFeed = 0
)

// errMpTxs is returned when a peer announces a sequence again with a different
// hash or count, or sends transactions that are not the ones it announced.
var errMpTxs = errors.New("mptxs does not match the interval the peer announced")

// intervalState is whether an announced interval is still to be downloaded,
// has been asked for, or will not be asked for.
type intervalState uint8

const (
	// intervalPending is waiting for a getmptxs.
	intervalPending intervalState = iota

	// intervalAsked was included in a getmptxs at askedAt.
	intervalAsked

	// intervalDone was delivered, or was not downloaded because it is at
	// the floor and its filter did not match.  A later mptxs for it is
	// ignored.
	intervalDone
)

// mpInterval is one interval of mempool transactions the feed has announced.
type mpInterval struct {
	hash    chainhash.Hash
	txCount uint32
	state   intervalState
	askedAt time.Time
}

// mempoolIntervals is the feed's intervals.  It decides which to download,
// asks for pending runs, and checks the transactions that come back.
type mempoolIntervals struct {
	mtx sync.Mutex

	floor   uint32
	entries func() [][]byte
	deliver func(tx *bchutil.Tx)
	now     func() time.Time

	// feedID is the feed peer, or noFeed.  intervals are the ones it has
	// announced that we still keep, and order is their sequence numbers as
	// they arrived, so the oldest can be dropped.
	feedID    int32
	intervals map[uint32]*mpInterval
	order     []uint32
}

// newMempoolIntervals returns an interval watcher.  floor is how many
// transactions an interval must hold before its filter is consulted; zero
// means DefaultMempoolFetchFloor.  entries returns the script keys to match
// a large interval's filter against, deliver takes each full transaction of
// an interval we asked for, and now is the clock asks are timed by.
func newMempoolIntervals(floor uint32, entries func() [][]byte,
	deliver func(tx *bchutil.Tx), now func() time.Time) *mempoolIntervals {

	if floor == 0 {
		floor = DefaultMempoolFetchFloor
	}
	return &mempoolIntervals{
		floor:     floor,
		entries:   entries,
		deliver:   deliver,
		now:       now,
		intervals: make(map[uint32]*mpInterval),
	}
}

// setFeed drops every interval recorded so far and takes id as the feed.  An
// id of 0 or less means there is no feed, because a connected peer's id is
// never 0.
func (m *mempoolIntervals) setFeed(id int32) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if id <= 0 {
		id = noFeed
	}
	m.feedID = id
	m.intervals = make(map[uint32]*mpInterval)
	m.order = nil
}

// feed returns the feed peer's id.
func (m *mempoolIntervals) feed() (int32, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.feedID == noFeed {
		return 0, false
	}
	return m.feedID, true
}

// onFilter records the interval msg announces from the feed and marks it
// pending when it is to be downloaded.  The same announcement again keeps
// the interval's state.  A different hash or count returns errMpTxs.  A peer
// that is not the feed, and a filter that is not a script filter, are ignored.
func (m *mempoolIntervals) onFilter(peer int32, msg *wire.MsgMpFilter) error {
	if msg.FilterType != wire.GCSFilterScripts {
		return nil
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.feedID == noFeed || peer != m.feedID {
		return nil
	}
	if iv := m.intervals[msg.Seq]; iv != nil {
		if iv.hash != msg.IntervalHash || iv.txCount != msg.TxCount {
			return errMpTxs
		}
		return nil
	}

	iv := &mpInterval{
		hash:    msg.IntervalHash,
		txCount: msg.TxCount,
		state:   intervalDone,
	}
	if m.fetch(msg) {
		iv.state = intervalPending
	}
	m.remember(msg.Seq, iv)
	return nil
}

// fetch reports whether the interval is downloaded.  One below the floor
// always is, and its filter is not read, so the request does not depend on
// what we watch.  One at the floor or above is downloaded only when the
// filter matches a script we watch.  A filter that cannot be read matches
// nothing.
func (m *mempoolIntervals) fetch(msg *wire.MsgMpFilter) bool {
	if msg.TxCount < m.floor {
		return true
	}

	// entries only reads the requester, which never calls back here, so
	// holding this lock across it cannot deadlock.
	entries := m.entries()
	if len(entries) == 0 {
		return false
	}
	filter, err := gcs.FromNBytes(builder.DefaultP, builder.DefaultM, msg.Data)
	if err != nil || filter.N() == 0 {
		return false
	}
	matched, err := filter.MatchAny(builder.DeriveKey(&msg.IntervalHash), entries)
	return err == nil && matched
}

// remember stores iv and drops the oldest interval past mpIntervalsKept,
// whatever state it is in.  A node keeps no more than that either.
func (m *mempoolIntervals) remember(seq uint32, iv *mpInterval) {
	m.intervals[seq] = iv
	m.order = append(m.order, seq)
	for len(m.order) > mpIntervalsKept {
		delete(m.intervals, m.order[0])
		m.order = m.order[1:]
	}
}

// next returns a getmptxs for the oldest run of pending intervals, after
// turning asks older than mpTxsWait back into pending.  take is called once,
// and only when there is a run; a false take leaves the run pending and
// returns nil.  There is no feed, or nothing is pending, when it returns nil
// without calling take.
func (m *mempoolIntervals) next(take func() bool) *wire.MsgGetMpTxs {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.feedID == noFeed {
		return nil
	}

	now := m.now()
	for _, iv := range m.intervals {
		if iv.state == intervalAsked && !now.Before(iv.askedAt.Add(mpTxsWait)) {
			iv.state = intervalPending
		}
	}
	first, ok := m.oldestPending()
	if !ok {
		return nil
	}
	last := m.runEnd(first)
	if !take() {
		return nil
	}
	for seq := first; ; seq++ {
		iv := m.intervals[seq]
		iv.state = intervalAsked
		iv.askedAt = now
		if seq == last {
			break
		}
	}
	return wire.NewMsgGetMpTxs(wire.GCSFilterScripts, first, last)
}

// oldestPending returns the smallest sequence number that is pending.
func (m *mempoolIntervals) oldestPending() (uint32, bool) {
	var first uint32
	found := false
	for seq, iv := range m.intervals {
		if iv.state != intervalPending {
			continue
		}
		if !found || seq < first {
			first = seq
			found = true
		}
	}
	return first, found
}

// runEnd returns the last sequence of the pending run that starts at first.
// The run stops at a gap or at an interval that is not pending, and it does
// not step past the largest sequence number.
func (m *mempoolIntervals) runEnd(first uint32) uint32 {
	last := first
	for last < ^uint32(0) {
		next := m.intervals[last+1]
		if next == nil || next.state != intervalPending {
			break
		}
		last++
	}
	return last
}

// onTxs checks the transactions peer sent for one interval against the
// interval the feed announced.  An interval that is not asked is ignored,
// including one that is already done.  A count or interval hash that differs
// returns errMpTxs and leaves the interval asked, so a later correct answer
// can still be delivered.  The full transactions of an accepted answer are
// handed to deliver without the lock held, and an entry that is only an id
// is skipped.
func (m *mempoolIntervals) onTxs(peer int32, msg *wire.MsgMpTxs) error {
	m.mtx.Lock()

	var iv *mpInterval
	if m.feedID != noFeed && peer == m.feedID {
		iv = m.intervals[msg.Seq]
	}
	if iv == nil || iv.state != intervalAsked {
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
	iv.state = intervalDone
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

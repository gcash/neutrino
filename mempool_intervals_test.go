package neutrino

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/gcash/bchd/btcjson"
	"github.com/gcash/bchd/chaincfg"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/txscript"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs/builder"
)

// intervalPrevScript is the locking script the test transactions spend. It is
// not a script any test watches, so a filter matches only an output that pays
// a watched script.
var intervalPrevScript = []byte{0x51}

// intervalTx returns a transaction that spends one output and pays script.
// n makes each transaction's id its own.
func intervalTx(script []byte, n uint32) *wire.MsgTx {
	var prev chainhash.Hash
	prev[0] = byte(n)
	prev[1] = byte(n >> 8)
	prev[2] = byte(n >> 16)
	prev[3] = byte(n >> 24)
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: prev, Index: n}, nil))
	tx.AddTxOut(wire.NewTxOut(1000, script, wire.TokenData{}))
	return tx
}

// mpIntervalMsgs returns the mpfilter of txs, numbered seq, and the mptxs a
// node would answer a request for that interval with. The filter is built the
// way a node builds one, keyed by the hash of the transaction ids in order.
func mpIntervalMsgs(t *testing.T, seq uint32, txs []*wire.MsgTx) (*wire.MsgMpFilter, *wire.MsgMpTxs) {
	t.Helper()

	ids := make([]chainhash.Hash, len(txs))
	entries := make([]wire.MpTx, len(txs))
	var prevOutScripts [][]byte
	for i, tx := range txs {
		ids[i] = tx.TxHash()
		entries[i] = wire.MpTx{Tx: tx, TxID: ids[i]}
		for range tx.TxIn {
			prevOutScripts = append(prevOutScripts, intervalPrevScript)
		}
	}
	hash := wire.MpIntervalHash(ids)
	filter, err := builder.BuildTxScriptFilter(&hash, txs, prevOutScripts)
	if err != nil {
		t.Fatalf("interval %d: %v", seq, err)
	}
	data, err := filter.NBytes()
	if err != nil {
		t.Fatalf("interval %d: %v", seq, err)
	}
	return &wire.MsgMpFilter{
			FilterType:   wire.GCSFilterScripts,
			Seq:          seq,
			IntervalHash: hash,
			TxCount:      uint32(len(txs)),
			Data:         data,
		}, &wire.MsgMpTxs{
			FilterType: wire.GCSFilterScripts,
			Seq:        seq,
			Txs:        entries,
		}
}

// intervalOf returns the messages for an interval of n transactions that pay
// script. idBase keeps their ids apart from those of another call.
func intervalOf(t *testing.T, seq uint32, n int, script []byte, idBase uint32) (*wire.MsgMpFilter, *wire.MsgMpTxs) {
	t.Helper()
	txs := make([]*wire.MsgTx, n)
	for i := range txs {
		txs[i] = intervalTx(script, idBase+uint32(i))
	}
	return mpIntervalMsgs(t, seq, txs)
}

// requireRun fails the test unless got asks for the intervals first to last.
func requireRun(t *testing.T, got *wire.MsgGetMpTxs, first, last uint32) {
	t.Helper()
	if got == nil || got.FilterType != wire.GCSFilterScripts ||
		got.FirstSeq != first || got.LastSeq != last {

		t.Fatalf("run %v, want intervals %d to %d", got, first, last)
	}
}

// allowMempoolScore is a take that always has room for a getmptxs.
func allowMempoolScore() bool { return true }

// intervalClock is a fixed clock for tests that do not move time.
func intervalClock() time.Time { return time.Unix(1_700_000_000, 0) }

// mustOnFilter records msg and fails the test if the peer misbehaves.
func mustOnFilter(t *testing.T, m *mempoolIntervals, peer int32, msg *wire.MsgMpFilter) {
	t.Helper()
	if err := m.onFilter(peer, msg); err != nil {
		t.Fatalf("interval %d: %v", msg.Seq, err)
	}
}

// sameEntries reports whether got and want hold the same entries, in any
// order.
func sameEntries(got, want [][]byte) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]int, len(want))
	for _, entry := range want {
		seen[string(entry)]++
	}
	for _, entry := range got {
		seen[string(entry)]--
		if seen[string(entry)] < 0 {
			return false
		}
	}
	return true
}

// TestMempoolIntervalSmallAlwaysFetched checks that an interval below the
// floor is asked for even when it pays nothing we watch. The filter is not
// what decides.
func TestMempoolIntervalSmallAlwaysFetched(t *testing.T) {
	watched := []byte{0x52, 0x01}
	other := []byte{0x53, 0x02}
	key := builder.ScriptKey(watched)
	m := newMempoolIntervals(10, func() [][]byte { return [][]byte{key[:]} },
		func(*bchutil.Tx) {}, intervalClock)
	m.setFeed(1)

	var id uint32 = 1
	for i, n := range []int{1, 5, 9} {
		seq := uint32(i + 1)
		msg, reply := intervalOf(t, seq, n, other, id)
		id += uint32(n)
		mustOnFilter(t, m, 1, msg)
		requireRun(t, m.next(allowMempoolScore), seq, seq)
		if err := m.onTxs(1, reply); err != nil {
			t.Fatalf("interval %d: %v", seq, err)
		}
	}
}

// askedRun is one getmptxs, the first and last sequence it names.
type askedRun struct {
	first uint32
	last  uint32
}

// TestMempoolIntervalRequestsDoNotDependOnWatched checks the privacy
// property: the same small intervals produce the same getmptxs from a feed
// that watches a script some of them pay and from a feed that watches
// nothing. Every count is below the floor, and the sequence numbers have gaps.
func TestMempoolIntervalRequestsDoNotDependOnWatched(t *testing.T) {
	watched := []byte{0x52, 0x0a}
	other := []byte{0x53, 0x0b}
	key := builder.ScriptKey(watched)

	var seqs []uint32
	for seq := uint32(1); len(seqs) < 30; seq++ {
		if seq%11 == 0 {
			continue
		}
		seqs = append(seqs, seq)
	}
	msgs := make(map[uint32]*wire.MsgMpFilter, len(seqs))
	replies := make(map[uint32]*wire.MsgMpTxs, len(seqs))
	var id uint32 = 1
	for i, seq := range seqs {
		script := other
		if i%7 == 0 {
			script = watched
		}
		count := (i % 20) + 1
		msg, reply := intervalOf(t, seq, count, script, id)
		id += uint32(count)
		msgs[seq] = msg
		replies[seq] = reply
	}

	drive := func(entries func() [][]byte) []askedRun {
		t.Helper()
		m := newMempoolIntervals(100, entries, func(*bchutil.Tx) {}, intervalClock)
		m.setFeed(1)
		var runs []askedRun
		for _, seq := range seqs {
			mustOnFilter(t, m, 1, msgs[seq])
			for {
				get := m.next(allowMempoolScore)
				if get == nil {
					break
				}
				if get.FilterType != wire.GCSFilterScripts || get.LastSeq < get.FirstSeq {
					t.Fatalf("run %v", get)
				}
				runs = append(runs, askedRun{get.FirstSeq, get.LastSeq})
				for s := get.FirstSeq; s <= get.LastSeq; s++ {
					reply := replies[s]
					if reply == nil {
						t.Fatalf("run %d-%d asks for %d, which was not announced",
							get.FirstSeq, get.LastSeq, s)
					}
					if err := m.onTxs(1, reply); err != nil {
						t.Fatalf("interval %d: %v", s, err)
					}
				}
			}
		}
		return runs
	}

	watchedRuns := drive(func() [][]byte { return [][]byte{key[:]} })
	blindRuns := drive(func() [][]byte { return nil })
	if len(watchedRuns) == 0 {
		t.Fatal("nothing was asked for")
	}
	if len(watchedRuns) != len(blindRuns) {
		t.Fatalf("watched %v, watching nothing %v", watchedRuns, blindRuns)
	}
	for i := range watchedRuns {
		if watchedRuns[i] != blindRuns[i] {
			t.Fatalf("run %d is %v when a script is watched and %v when nothing is",
				i, watchedRuns[i], blindRuns[i])
		}
	}
	announced := make(map[uint32]bool, len(seqs))
	for _, seq := range seqs {
		announced[seq] = true
	}
	covered := make(map[uint32]bool, len(seqs))
	for _, run := range watchedRuns {
		for seq := run.first; seq <= run.last; seq++ {
			if !announced[seq] {
				t.Fatalf("run %d-%d includes %d, which was not announced",
					run.first, run.last, seq)
			}
			covered[seq] = true
		}
	}
	for _, seq := range seqs {
		if !covered[seq] {
			t.Fatalf("sequence %d was not asked for", seq)
		}
	}
}

// TestMempoolIntervalLargeOnlyOnMatch checks that an interval at or above the
// floor is asked for only when its filter matches a watched script, and that
// one below the floor is asked for either way. A filter that cannot be read
// is no match.
func TestMempoolIntervalLargeOnlyOnMatch(t *testing.T) {
	watched := []byte{0x52, 0x11}
	other := []byte{0x53, 0x12}
	key := builder.ScriptKey(watched)
	m := newMempoolIntervals(10, func() [][]byte { return [][]byte{key[:]} },
		func(*bchutil.Tx) {}, intervalClock)
	m.setFeed(1)

	var seq uint32
	var id uint32 = 1
	check := func(n int, script []byte, asked bool) {
		t.Helper()
		seq++
		msg, reply := intervalOf(t, seq, n, script, id)
		id += uint32(n)
		mustOnFilter(t, m, 1, msg)
		got := m.next(allowMempoolScore)
		if !asked {
			if got != nil {
				t.Fatalf("interval of %d paying %x was asked for: %v", n, script, got)
			}
			return
		}
		requireRun(t, got, seq, seq)
		if err := m.onTxs(1, reply); err != nil {
			t.Fatalf("interval %d: %v", seq, err)
		}
	}

	check(10, other, false)
	check(50, other, false)
	check(9, other, true)
	check(9, watched, true)
	check(10, watched, true)
	check(50, watched, true)

	seq++
	mustOnFilter(t, m, 1, &wire.MsgMpFilter{
		FilterType: wire.GCSFilterScripts,
		Seq:        seq,
		TxCount:    10,
		Data:       []byte{0xff},
	})
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("an unreadable filter was asked for: %v", got)
	}
}

// TestMempoolIntervalRuns checks that pending intervals are asked for in
// gap-free runs, oldest first, and that a large interval that did not match
// is left out and splits the run around it.
func TestMempoolIntervalRuns(t *testing.T) {
	other := []byte{0x53, 0x21}
	m := newMempoolIntervals(10, func() [][]byte { return nil },
		func(*bchutil.Tx) {}, intervalClock)
	m.setFeed(1)
	for _, seq := range []uint32{5, 6, 7, 9} {
		msg, _ := intervalOf(t, seq, 1, other, seq*10)
		mustOnFilter(t, m, 1, msg)
	}
	requireRun(t, m.next(allowMempoolScore), 5, 7)
	requireRun(t, m.next(allowMempoolScore), 9, 9)
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("a third run: %v", got)
	}

	split := newMempoolIntervals(10, func() [][]byte { return nil },
		func(*bchutil.Tx) {}, intervalClock)
	split.setFeed(1)
	small, _ := intervalOf(t, 1, 1, other, 1)
	large, _ := intervalOf(t, 2, 10, other, 100)
	tail, _ := intervalOf(t, 3, 1, other, 200)
	mustOnFilter(t, split, 1, small)
	mustOnFilter(t, split, 1, large)
	mustOnFilter(t, split, 1, tail)
	requireRun(t, split.next(allowMempoolScore), 1, 1)
	requireRun(t, split.next(allowMempoolScore), 3, 3)
	if got := split.next(allowMempoolScore); got != nil {
		t.Fatalf("the interval that did not match was asked for: %v", got)
	}
}

// TestMempoolIntervalWaitsForScore checks that a run stays pending when the
// ban score has no room, that it is asked for once the score does, and that
// take is not called when nothing is pending.
func TestMempoolIntervalWaitsForScore(t *testing.T) {
	other := []byte{0x53, 0x31}
	var delivered int
	m := newMempoolIntervals(10, func() [][]byte { return nil },
		func(*bchutil.Tx) { delivered++ }, intervalClock)
	m.setFeed(1)

	calls := 0
	if got := m.next(func() bool { calls++; return true }); got != nil || calls != 0 {
		t.Fatalf("nothing pending: run %v, take called %d times", got, calls)
	}

	var replies []*wire.MsgMpTxs
	for seq := uint32(4); seq <= 6; seq++ {
		msg, reply := intervalOf(t, seq, 1, other, seq)
		mustOnFilter(t, m, 1, msg)
		replies = append(replies, reply)
	}
	calls = 0
	if got := m.next(func() bool { calls++; return false }); got != nil || calls != 1 {
		t.Fatalf("no room: run %v, take called %d times", got, calls)
	}
	if err := m.onTxs(1, replies[0]); err != nil || delivered != 0 {
		t.Fatalf("asked with no room: delivered %d, err %v", delivered, err)
	}
	calls = 0
	requireRun(t, m.next(func() bool { calls++; return true }), 4, 6)
	if calls != 1 {
		t.Fatalf("take was called %d times", calls)
	}
}

// TestMempoolIntervalAskedAgain checks that an asked interval is not asked
// again before 30 seconds and is asked again once that has passed, and that
// an interval that was answered is not.
func TestMempoolIntervalAskedAgain(t *testing.T) {
	other := []byte{0x53, 0x41}
	now := intervalClock()
	m := newMempoolIntervals(10, func() [][]byte { return nil },
		func(*bchutil.Tx) {}, func() time.Time { return now })
	m.setFeed(1)

	first, firstReply := intervalOf(t, 1, 1, other, 1)
	second, secondReply := intervalOf(t, 2, 1, other, 2)
	mustOnFilter(t, m, 1, first)
	mustOnFilter(t, m, 1, second)
	requireRun(t, m.next(allowMempoolScore), 1, 2)
	if err := m.onTxs(1, firstReply); err != nil {
		t.Fatal(err)
	}

	now = now.Add(mpTxsWait - time.Second)
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("asked again after %v: %v", mpTxsWait-time.Second, got)
	}
	now = now.Add(time.Second)
	requireRun(t, m.next(allowMempoolScore), 2, 2)
	if err := m.onTxs(1, secondReply); err != nil {
		t.Fatal(err)
	}

	now = now.Add(mpTxsWait)
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("an answered interval was asked again: %v", got)
	}
}

// TestMempoolIntervalTxs checks that the transactions of an asked interval are
// delivered in order, that a transaction sent as only an id is skipped, and
// that an interval we did not ask for or that doesn't match its announcement
// delivers nothing. deliver is called without the interval lock.
func TestMempoolIntervalTxs(t *testing.T) {
	watched := []byte{0x52, 0x51}
	key := builder.ScriptKey(watched)
	var got []*bchutil.Tx
	var m *mempoolIntervals
	m = newMempoolIntervals(1, func() [][]byte { return [][]byte{key[:]} },
		func(tx *bchutil.Tx) {
			if !m.mtx.TryLock() {
				t.Errorf("deliver called with the interval lock held")
			} else {
				m.mtx.Unlock()
			}
			got = append(got, tx)
		}, intervalClock)
	m.setFeed(1)

	txs := []*wire.MsgTx{
		intervalTx(watched, 1),
		intervalTx(watched, 2),
		intervalTx(watched, 3),
	}
	filt, reply := mpIntervalMsgs(t, 9, txs)
	mustOnFilter(t, m, 1, filt)
	requireRun(t, m.next(allowMempoolScore), 9, 9)
	// The middle transaction has left the mempool: only its id remains,
	// which still counts toward the interval's hash.
	reply.Txs[1].Tx = nil
	if err := m.onTxs(1, reply); err != nil {
		t.Fatalf("asked interval: %v", err)
	}
	if len(got) != 2 || *got[0].Hash() != txs[0].TxHash() ||
		*got[1].Hash() != txs[2].TxHash() {

		t.Fatalf("delivered %#v, want %v then %v", got, txs[0].TxHash(),
			txs[2].TxHash())
	}
	if err := m.onTxs(1, reply); err != nil || len(got) != 2 {
		t.Fatalf("the interval was delivered again: %d, err %v", len(got), err)
	}
	if err := m.onTxs(99, reply); err != nil || len(got) != 2 {
		t.Fatalf("a peer that is not the feed was delivered: %d, err %v",
			len(got), err)
	}

	miss, missTxs := mpIntervalMsgs(t, 10, []*wire.MsgTx{intervalTx([]byte{0x53}, 4)})
	mustOnFilter(t, m, 1, miss)
	if gotRun := m.next(allowMempoolScore); gotRun != nil {
		t.Fatalf("interval 10 matched: %v", gotRun)
	}
	if err := m.onTxs(1, missTxs); err != nil || len(got) != 2 {
		t.Fatalf("an interval that was not asked for was delivered: %d, err %v",
			len(got), err)
	}

	badFilt, badReply := mpIntervalMsgs(t, 11, []*wire.MsgTx{
		intervalTx(watched, 5), intervalTx(watched, 6),
	})
	mustOnFilter(t, m, 1, badFilt)
	requireRun(t, m.next(allowMempoolScore), 11, 11)
	kept := badReply.Txs[0]
	badReply.Txs[0].Tx = intervalTx(watched, 7)
	badReply.Txs[0].TxID = badReply.Txs[0].Tx.TxHash()
	err := m.onTxs(1, badReply)
	if !errors.Is(err, errMpTxs) || len(got) != 2 {
		t.Fatalf("replaced transaction: delivered %d, err %v", len(got), err)
	}
	// The mismatch is not an answer, so the announced transactions still
	// belong to the interval we asked for.
	badReply.Txs[0] = kept
	if err := m.onTxs(1, badReply); err != nil || len(got) != 4 {
		t.Fatalf("the announced transactions: delivered %d, err %v", len(got), err)
	}

	shortFilt, shortReply := mpIntervalMsgs(t, 12, []*wire.MsgTx{
		intervalTx(watched, 8), intervalTx(watched, 9),
	})
	mustOnFilter(t, m, 1, shortFilt)
	if m.next(allowMempoolScore) == nil {
		t.Fatal("interval 12 did not match")
	}
	shortReply.Txs = shortReply.Txs[:1]
	err = m.onTxs(1, shortReply)
	if !errors.Is(err, errMpTxs) || len(got) != 4 {
		t.Fatalf("a transaction left out: delivered %d, err %v", len(got), err)
	}
}

// TestMempoolIntervalAnnouncedAgain checks that announcing a sequence again
// with the same hash and count changes nothing, and that a different hash or
// count is a misbehaving peer. An asked interval stays asked, and a done one
// stays done and is not asked for again.
func TestMempoolIntervalAnnouncedAgain(t *testing.T) {
	watched := []byte{0x52, 0x61}
	key := builder.ScriptKey(watched)
	now := intervalClock()
	var got int
	m := newMempoolIntervals(1, func() [][]byte { return [][]byte{key[:]} },
		func(*bchutil.Tx) { got++ }, func() time.Time { return now })
	m.setFeed(1)

	msg, reply := intervalOf(t, 4, 1, watched, 1)
	mustOnFilter(t, m, 1, msg)
	requireRun(t, m.next(allowMempoolScore), 4, 4)
	mustOnFilter(t, m, 1, msg)
	if again := m.next(allowMempoolScore); again != nil {
		t.Fatalf("an asked interval was asked again: %v", again)
	}

	altered := *msg
	altered.IntervalHash[0] ^= 0xff
	if err := m.onFilter(1, &altered); !errors.Is(err, errMpTxs) {
		t.Fatalf("different hash: %v", err)
	}
	if err := m.onTxs(1, reply); err != nil || got != 1 {
		t.Fatalf("original answer: delivered %d, err %v", got, err)
	}

	mustOnFilter(t, m, 1, msg)
	now = now.Add(mpTxsWait)
	if again := m.next(allowMempoolScore); again != nil {
		t.Fatalf("a delivered interval was asked again: %v", again)
	}
	counted := *msg
	counted.TxCount++
	if err := m.onFilter(1, &counted); !errors.Is(err, errMpTxs) {
		t.Fatalf("different count: %v", err)
	}

	second, secondReply := intervalOf(t, 5, 2, watched, 10)
	mustOnFilter(t, m, 1, second)
	requireRun(t, m.next(allowMempoolScore), 5, 5)
	counted = *second
	counted.TxCount++
	if err := m.onFilter(1, &counted); !errors.Is(err, errMpTxs) {
		t.Fatalf("different count while asked: %v", err)
	}
	if err := m.onTxs(1, secondReply); err != nil || got != 3 {
		t.Fatalf("the count was replaced: delivered %d, err %v", got, err)
	}
}

// TestMempoolIntervalFeed checks that a peer that is not the feed is ignored
// and that setFeed drops the intervals recorded for the previous one.
func TestMempoolIntervalFeed(t *testing.T) {
	other := []byte{0x53, 0x71}
	var delivered int
	m := newMempoolIntervals(10, func() [][]byte { return nil },
		func(*bchutil.Tx) { delivered++ }, intervalClock)
	msg, reply := intervalOf(t, 3, 1, other, 1)

	if _, ok := m.feed(); ok {
		t.Fatal("a new watcher has a feed")
	}
	mustOnFilter(t, m, 1, msg)
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("no feed was asked: %v", got)
	}

	m.setFeed(1)
	if id, ok := m.feed(); !ok || id != 1 {
		t.Fatalf("feed is %d, %v", id, ok)
	}
	mustOnFilter(t, m, 2, msg)
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("a peer that is not the feed was asked: %v", got)
	}
	mustOnFilter(t, m, 1, msg)
	requireRun(t, m.next(allowMempoolScore), 3, 3)

	m.setFeed(2)
	if id, ok := m.feed(); !ok || id != 2 {
		t.Fatalf("feed is %d, %v", id, ok)
	}
	if err := m.onTxs(1, reply); err != nil || delivered != 0 {
		t.Fatalf("the old feed delivered %d, err %v", delivered, err)
	}
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("the old feed's intervals survived: %v", got)
	}
	mustOnFilter(t, m, 2, msg)
	requireRun(t, m.next(allowMempoolScore), 3, 3)

	m.setFeed(0)
	if _, ok := m.feed(); ok {
		t.Fatal("0 is a feed")
	}
	m.setFeed(2)
	m.setFeed(-3)
	if _, ok := m.feed(); ok {
		t.Fatal("a negative id is a feed")
	}
}

// TestMempoolIntervalsKept checks that the 721st interval drops the oldest
// and leaves the one after it.
func TestMempoolIntervalsKept(t *testing.T) {
	m := newMempoolIntervals(10, func() [][]byte { return nil },
		func(*bchutil.Tx) {}, intervalClock)
	m.setFeed(1)

	hashOf := func(seq uint32) chainhash.Hash {
		var hash chainhash.Hash
		hash[0] = byte(seq)
		hash[1] = byte(seq >> 8)
		hash[2] = byte(seq >> 16)
		hash[3] = byte(seq >> 24)
		return hash
	}
	msg := &wire.MsgMpFilter{
		FilterType: wire.GCSFilterScripts,
		TxCount:    1,
	}
	for seq := uint32(1); seq <= mpIntervalsKept; seq++ {
		msg.Seq = seq
		msg.IntervalHash = hashOf(seq)
		mustOnFilter(t, m, 1, msg)
	}
	msg.Seq = 1
	msg.IntervalHash = hashOf(1)
	msg.IntervalHash[0] ^= 0xff
	if err := m.onFilter(1, msg); !errors.Is(err, errMpTxs) {
		t.Fatalf("the oldest of %d intervals: %v", mpIntervalsKept, err)
	}

	msg.Seq = mpIntervalsKept + 1
	msg.IntervalHash = hashOf(msg.Seq)
	mustOnFilter(t, m, 1, msg)

	msg.Seq = 2
	msg.IntervalHash = hashOf(2)
	msg.IntervalHash[0] ^= 0xff
	if err := m.onFilter(1, msg); !errors.Is(err, errMpTxs) {
		t.Fatalf("the interval after the oldest was dropped: %v", err)
	}
	msg.Seq = 1
	msg.IntervalHash = hashOf(1)
	msg.IntervalHash[0] ^= 0xff
	if err := m.onFilter(1, msg); err != nil {
		t.Fatalf("the dropped interval was still recorded: %v", err)
	}
}

// TestMempoolIntervalRegular checks that a regular filter is ignored, and
// that it is not kept to extend a later run.
func TestMempoolIntervalRegular(t *testing.T) {
	watched := []byte{0x52, 0x31}
	key := builder.ScriptKey(watched)
	m := newMempoolIntervals(10, func() [][]byte { return [][]byte{key[:]} },
		func(*bchutil.Tx) {}, intervalClock)
	m.setFeed(1)

	regular, _ := intervalOf(t, 4, 1, watched, 1)
	regular.FilterType = wire.GCSFilterRegular
	regular.TxCount = 1
	mustOnFilter(t, m, 1, regular)
	if got := m.next(allowMempoolScore); got != nil {
		t.Fatalf("a regular filter was asked for: %v", got)
	}
	hit, _ := intervalOf(t, 5, 1, watched, 2)
	mustOnFilter(t, m, 1, hit)
	requireRun(t, m.next(allowMempoolScore), 5, 5)
}

// TestMempoolEntryList checks that entryList returns the entries the
// requester was given, and that changing the returned slices does not change
// what it watches.
func TestMempoolEntryList(t *testing.T) {
	r := newMempoolRequester(nil, nil, nil)
	if len(r.entryList()) != 0 {
		t.Fatal("a new requester watches entries")
	}
	first := []byte{1, 2, 3}
	second := []byte{4, 5}
	r.request([][]byte{first, second})
	got := r.entryList()
	if !sameEntries(got, [][]byte{first, second}) {
		t.Fatalf("entries %x", got)
	}
	got[0][0] ^= 0xff
	if !bytes.Equal(first, []byte{1, 2, 3}) ||
		!sameEntries(r.entryList(), [][]byte{first, second}) {

		t.Fatal("entryList did not return a copy of the entries")
	}
}

// TestDeliverMempoolTxOnce checks that a transaction of an interval that is
// fetched again, as part of a later run, is reported once.
func TestDeliverMempoolTxOnce(t *testing.T) {
	addr, err := bchutil.NewAddressPubKeyHash(bytes.Repeat([]byte{7}, 20), &chaincfg.SimNetParams)
	if err != nil {
		t.Fatal(err)
	}
	script, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatal(err)
	}
	tx := wire.NewMsgTx(2)
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{1}, 0), nil))
	tx.AddTxOut(wire.NewTxOut(1000, script, wire.TokenData{}))

	s := &ChainService{mempool: NewMempool()}
	reported := 0
	s.mempool.RegisterCallback(func(*bchutil.Tx, *btcjson.BlockDetails) { reported++ })
	s.mempool.NotifyReceived([]bchutil.Address{addr})
	for range 3 {
		s.deliverMempoolTx(bchutil.NewTx(tx))
	}
	if reported != 1 {
		t.Errorf("the transaction was reported %d times", reported)
	}
}

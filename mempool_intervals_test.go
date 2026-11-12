package neutrino

import (
	"bytes"
	"errors"
	"testing"

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

// requireRun fails the test unless got asks for the intervals first to last.
func requireRun(t *testing.T, got *wire.MsgGetMpTxs, first, last uint32) {
	t.Helper()
	if got == nil || got.FilterType != wire.GCSFilterScripts ||
		got.FirstSeq != first || got.LastSeq != last {

		t.Fatalf("run %v, want intervals %d to %d", got, first, last)
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

// TestMempoolIntervalMatch checks that a script filter is asked for only when
// it matches a watched script and the peer can take the request, and that an
// interval which could not be asked for stays recorded for a later one.
func TestMempoolIntervalMatch(t *testing.T) {
	watched := []byte{0x52, 0x01}
	other := []byte{0x53, 0x02}
	key := builder.ScriptKey(watched)
	var entries [][]byte
	var delivered int
	m := newMempoolIntervals(func() [][]byte { return entries },
		func(*bchutil.Tx) { delivered++ })

	entries = [][]byte{key[:]}
	hit, _ := mpIntervalMsgs(t, 1, []*wire.MsgTx{intervalTx(watched, 1)})
	requireRun(t, m.onFilter("peer", hit, true), 1, 1)

	miss, _ := mpIntervalMsgs(t, 2, []*wire.MsgTx{intervalTx(other, 2)})
	if got := m.onFilter("peer", miss, true); got != nil {
		t.Fatalf("a filter that matches nothing was asked for: %v", got)
	}

	entries = nil
	unwatched, _ := mpIntervalMsgs(t, 3, []*wire.MsgTx{intervalTx(watched, 3)})
	if got := m.onFilter("peer", unwatched, true); got != nil {
		t.Fatalf("a filter was asked for with nothing watched: %v", got)
	}

	entries = [][]byte{key[:]}
	held, heldTxs := mpIntervalMsgs(t, 4, []*wire.MsgTx{intervalTx(watched, 4)})
	if got := m.onFilter("peer", held, false); got != nil {
		t.Fatalf("a match was asked for when the peer can't take it: %v", got)
	}
	if err := m.onTxs("peer", heldTxs); err != nil || delivered != 0 {
		t.Fatalf("the interval that was not asked for: delivered %d, err %v",
			delivered, err)
	}

	later, _ := mpIntervalMsgs(t, 5, []*wire.MsgTx{intervalTx(watched, 5)})
	requireRun(t, m.onFilter("peer", later, true), 1, 5)
}

// TestMempoolIntervalRun checks how far a run reaches back over the intervals
// a peer has sent: through enough transactions, and no further than a gap or
// the oldest interval the peer sent.
func TestMempoolIntervalRun(t *testing.T) {
	prev := mpRunMinTxs
	mpRunMinTxs = 10
	defer func() { mpRunMinTxs = prev }()

	watched := []byte{0x52, 0x0a}
	other := []byte{0x53, 0x0b}
	key := builder.ScriptKey(watched)
	entries := [][]byte{key[:]}

	// four returns the filter of four transactions numbered seq. They pay
	// script, and none of them is a transaction of another call.
	four := func(m *mempoolIntervals, seq uint32, script []byte) *wire.MsgGetMpTxs {
		t.Helper()
		txs := make([]*wire.MsgTx, 4)
		for i := range txs {
			txs[i] = intervalTx(script, seq*4+uint32(i))
		}
		msg, _ := mpIntervalMsgs(t, seq, txs)
		return m.onFilter("peer", msg, true)
	}

	m := newMempoolIntervals(func() [][]byte { return entries }, func(*bchutil.Tx) {})
	for _, seq := range []uint32{5, 6, 7} {
		if got := four(m, seq, other); got != nil {
			t.Fatalf("interval %d matched nothing we watch: %v", seq, got)
		}
	}
	requireRun(t, four(m, 8, watched), 6, 8)

	gapped := newMempoolIntervals(func() [][]byte { return entries }, func(*bchutil.Tx) {})
	if got := four(gapped, 5, other); got != nil {
		t.Fatalf("interval 5 matched: %v", got)
	}
	if got := four(gapped, 7, other); got != nil {
		t.Fatalf("interval 7 matched: %v", got)
	}
	requireRun(t, four(gapped, 8, watched), 7, 8)

	only := newMempoolIntervals(func() [][]byte { return entries }, func(*bchutil.Tx) {})
	requireRun(t, four(only, 8, watched), 8, 8)
}

// TestMempoolIntervalTxs checks that the transactions of an asked interval are
// delivered in order, that a transaction sent as only an id is skipped, and
// that an interval we did not ask for or that doesn't match its announcement
// delivers nothing.
func TestMempoolIntervalTxs(t *testing.T) {
	watched := []byte{0x52, 0x11}
	key := builder.ScriptKey(watched)
	var got []*bchutil.Tx
	var m *mempoolIntervals
	m = newMempoolIntervals(func() [][]byte { return [][]byte{key[:]} },
		func(tx *bchutil.Tx) {
			if !m.mtx.TryLock() {
				t.Errorf("deliver called with the interval lock held")
			} else {
				m.mtx.Unlock()
			}
			got = append(got, tx)
		})

	txs := []*wire.MsgTx{
		intervalTx(watched, 1),
		intervalTx(watched, 2),
		intervalTx(watched, 3),
	}
	filt, reply := mpIntervalMsgs(t, 9, txs)
	requireRun(t, m.onFilter("p", filt, true), 9, 9)
	// The middle transaction has left the mempool: only its id remains,
	// which still counts toward the interval's hash.
	reply.Txs[1].Tx = nil
	if err := m.onTxs("p", reply); err != nil {
		t.Fatalf("asked interval: %v", err)
	}
	if len(got) != 2 || *got[0].Hash() != txs[0].TxHash() ||
		*got[1].Hash() != txs[2].TxHash() {

		t.Fatalf("delivered %#v, want %v then %v", got, txs[0].TxHash(),
			txs[2].TxHash())
	}
	if err := m.onTxs("p", reply); err != nil || len(got) != 2 {
		t.Fatalf("the interval was delivered again: %d, err %v", len(got), err)
	}
	if err := m.onTxs("nobody", reply); err != nil || len(got) != 2 {
		t.Fatalf("an unknown peer was delivered: %d, err %v", len(got), err)
	}

	miss, missTxs := mpIntervalMsgs(t, 10, []*wire.MsgTx{intervalTx([]byte{0x53}, 4)})
	if got := m.onFilter("p", miss, true); got != nil {
		t.Fatalf("interval 10 matched: %v", got)
	}
	if err := m.onTxs("p", missTxs); err != nil || len(got) != 2 {
		t.Fatalf("an interval that was not asked for was delivered: %d, err %v",
			len(got), err)
	}

	badFilt, badReply := mpIntervalMsgs(t, 11, []*wire.MsgTx{
		intervalTx(watched, 5), intervalTx(watched, 6),
	})
	requireRun(t, m.onFilter("p", badFilt, true), 9, 11)
	kept := badReply.Txs[0]
	badReply.Txs[0].Tx = intervalTx(watched, 7)
	badReply.Txs[0].TxID = badReply.Txs[0].Tx.TxHash()
	err := m.onTxs("p", badReply)
	if !errors.Is(err, errMpTxs) || len(got) != 2 {
		t.Fatalf("replaced transaction: delivered %d, err %v", len(got), err)
	}
	// The mismatch is not an answer, so the announced transactions still
	// belong to the interval we asked for.
	badReply.Txs[0] = kept
	if err := m.onTxs("p", badReply); err != nil || len(got) != 4 {
		t.Fatalf("the announced transactions: delivered %d, err %v", len(got), err)
	}

	shortFilt, shortReply := mpIntervalMsgs(t, 12, []*wire.MsgTx{
		intervalTx(watched, 8), intervalTx(watched, 9),
	})
	if m.onFilter("p", shortFilt, true) == nil {
		t.Fatal("interval 12 did not match")
	}
	shortReply.Txs = shortReply.Txs[:1]
	err = m.onTxs("p", shortReply)
	if !errors.Is(err, errMpTxs) || len(got) != 4 {
		t.Fatalf("a transaction left out: delivered %d, err %v", len(got), err)
	}
}

// TestMempoolIntervalsForgotten checks that a peer's intervals past
// mpIntervalsKept are dropped, oldest first, and that forget drops the rest.
func TestMempoolIntervalsForgotten(t *testing.T) {
	watched := []byte{0x52, 0x21}
	other := []byte{0x53, 0x22}
	key := builder.ScriptKey(watched)
	m := newMempoolIntervals(func() [][]byte { return [][]byte{key[:]} },
		func(*bchutil.Tx) {})
	m.forget("nobody")

	filler, _ := mpIntervalMsgs(t, 1, []*wire.MsgTx{intervalTx(other, 1)})
	for seq := uint32(1); seq <= mpIntervalsKept; seq++ {
		filler.Seq = seq
		m.onFilter("peer", filler, true)
	}
	hit, _ := mpIntervalMsgs(t, mpIntervalsKept+1, []*wire.MsgTx{intervalTx(watched, 2)})
	requireRun(t, m.onFilter("peer", hit, true), 2, mpIntervalsKept+1)

	m.forget("peer")
	again, _ := mpIntervalMsgs(t, mpIntervalsKept+2, []*wire.MsgTx{intervalTx(watched, 3)})
	requireRun(t, m.onFilter("peer", again, true), mpIntervalsKept+2, mpIntervalsKept+2)
}

// TestMempoolIntervalRegular checks that a regular filter is ignored, and
// that it is not kept to extend a later run.
func TestMempoolIntervalRegular(t *testing.T) {
	watched := []byte{0x52, 0x31}
	key := builder.ScriptKey(watched)
	m := newMempoolIntervals(func() [][]byte { return [][]byte{key[:]} },
		func(*bchutil.Tx) {})

	regular, _ := mpIntervalMsgs(t, 4, []*wire.MsgTx{intervalTx(watched, 1)})
	regular.FilterType = wire.GCSFilterRegular
	regular.TxCount = 5000
	if got := m.onFilter("p", regular, true); got != nil {
		t.Fatalf("a regular filter was asked for: %v", got)
	}
	hit, _ := mpIntervalMsgs(t, 5, []*wire.MsgTx{intervalTx(watched, 2)})
	requireRun(t, m.onFilter("p", hit, true), 5, 5)
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

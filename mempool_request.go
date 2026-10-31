package neutrino

import (
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

const (
	// mempoolFilterBanScore and mempoolBanScore are what bchd adds to a
	// peer's ban score for a getcfmempool and a mempool message from 0.22.5
	// on, and oldMempoolBanScore what it added for each before.  bchd keeps
	// the score for each connection, halves it every minute, and bans a
	// peer whose score passes 100.
	mempoolFilterBanScore = 10
	mempoolBanScore       = 20
	oldMempoolBanScore    = 33

	// maxMempoolBanScore is the highest ban score that our requests for
	// the mempool may leave a peer with, short of the 100 it bans at.
	maxMempoolBanScore = 90

	// mempoolRequestTick is how often the requester checks whether it can
	// ask for the mempool, and mempoolWakeGap how much later than that a
	// tick must come by the wall clock for the requester to take it that
	// the process was suspended, as a phone suspends it when it sleeps.
	mempoolRequestTick = time.Second
	mempoolWakeGap     = 15 * time.Second

	// mempoolDropWindow is how soon after we asked a peer for the mempool
	// it must disconnect for us to take it that it banned us, as a peer
	// whose ban threshold is lower than bchd's default would.
	mempoolDropWindow = 5 * time.Second
)

// mempoolRequester asks a peer for the transactions in its mempool that pay a
// script or spend an outpoint we watch: it asks for the filter of the mempool,
// and when that matches, for the mempool, whose transactions the block manager
// then fetches as it does announced ones.  It asks once it's told to and the
// block headers are synced, as the transactions announced before then aren't
// fetched, and again when we regain peers after having none, or when the clock
// shows that the process was suspended, as transactions sent meanwhile weren't
// announced to us.  It mirrors the ban score each peer keeps for us, and asks a
// peer only while that leaves the score at maxMempoolBanScore or below, so that
// a request no peer can take yet waits for one that can, rather than getting
// us banned or being dropped.
type mempoolRequester struct {
	peers  func() []*ServerPeer
	synced func() bool
	now    func() time.Time
	quit   <-chan struct{}

	// timeout is how long a peer has to send the filter of its mempool,
	// and dropWindow is mempoolDropWindow.
	timeout    time.Duration
	dropWindow time.Duration

	// wake is signalled when a request is wanted.
	wake chan struct{}

	// mtx guards the fields below.  entries are the scripts and the
	// serialized outpoints we watch, as the filter of a mempool holds
	// them.  wanted is set while a request is wanted, and wants counts the
	// times it was set, so that a request that comes while the peers are
	// asked isn't taken as answered.  dropped holds the hosts of the peers
	// that disconnected right after a request.
	mtx     sync.Mutex
	entries map[string]struct{}
	wanted  bool
	wants   uint64
	dropped map[string]struct{}
}

// newMempoolRequester returns a requester that asks the peers that peers
// returns, once synced returns true, until quit is closed.
func newMempoolRequester(peers func() []*ServerPeer, synced func() bool,
	quit <-chan struct{}) *mempoolRequester {

	return &mempoolRequester{
		peers:      peers,
		synced:     synced,
		now:        time.Now,
		quit:       quit,
		timeout:    QueryTimeout,
		dropWindow: mempoolDropWindow,
		wake:       make(chan struct{}, 1),
		entries:    make(map[string]struct{}),
		dropped:    make(map[string]struct{}),
	}
}

// request adds entries, scripts and serialized outpoints, to those we watch,
// and asks for the mempool.
func (r *mempoolRequester) request(entries [][]byte) {
	r.mtx.Lock()
	for _, entry := range entries {
		r.entries[string(entry)] = struct{}{}
	}
	r.mtx.Unlock()
	r.want()
}

// want asks for the mempool, if there is anything we watch.
func (r *mempoolRequester) want() {
	r.mtx.Lock()
	if len(r.entries) > 0 {
		r.wanted = true
		r.wants++
	}
	r.mtx.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// run asks for the mempool when it's wanted and a peer can take the request,
// until quit is closed.  It must be run in a goroutine.
func (r *mempoolRequester) run() {
	ticker := time.NewTicker(mempoolRequestTick)
	defer ticker.Stop()

	// Timers don't count the time a suspended process sleeps, so a tick
	// that comes much later than the last by the wall clock means the
	// process was suspended in between.
	last := r.now().Round(0)
	for {
		select {
		case <-r.wake:
		case <-ticker.C:
			if r.now().Round(0).Sub(last) > mempoolWakeGap {
				log.Debugf("Asking for the mempool again, as the " +
					"process was suspended")
				r.want()
			}
		case <-r.quit:
			return
		}
		r.attempt()
		last = r.now().Round(0)
	}
}

// attempt asks a peer for the mempool if a request is wanted, the block
// headers are synced, and a peer can take it, trying the next peer that can
// when one doesn't answer.
func (r *mempoolRequester) attempt() {
	r.mtx.Lock()
	wanted, wants := r.wanted, r.wants
	entries := make([][]byte, 0, len(r.entries))
	for entry := range r.entries {
		entries = append(entries, []byte(entry))
	}
	r.mtx.Unlock()

	if !wanted || !r.synced() {
		return
	}
	for _, sp := range r.candidates() {
		if !r.ask(sp, entries) {
			continue
		}
		r.mtx.Lock()
		if r.wants == wants {
			r.wanted = false
		}
		r.mtx.Unlock()
		return
	}
}

// candidates returns the connected peers that serve the filter of their
// mempool and their mempool, didn't disconnect right after a request, and can
// take a request without their ban score for us passing maxMempoolBanScore,
// those with the lowest score first.
func (r *mempoolRequester) candidates() []*ServerPeer {
	type candidate struct {
		sp    *ServerPeer
		score uint32
	}
	var candidates []candidate
	for _, sp := range r.peers() {
		if sp == nil || !sp.Connected() || r.droppedUs(sp) {
			continue
		}
		services := wire.SFNodeCF | wire.SFNodeBloom
		if sp.Services()&services != services {
			continue
		}
		filterScore, mempoolScore := mempoolBanScores(sp.UserAgent())
		score := sp.mempoolScore.Int()
		if score+filterScore+mempoolScore > maxMempoolBanScore {
			continue
		}
		candidates = append(candidates, candidate{sp, score})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].score < candidates[j].score
	})

	peers := make([]*ServerPeer, len(candidates))
	for i, c := range candidates {
		peers[i] = c.sp
	}
	return peers
}

// ask asks sp for the filter of its mempool and, when it matches entries, for
// its mempool, and returns whether sp sent the filter.
func (r *mempoolRequester) ask(sp *ServerPeer, entries [][]byte) bool {
	filterScore, mempoolScore := mempoolBanScores(sp.UserAgent())

	msgChan := make(chan spMsg)
	subQuit := make(chan struct{})
	subscription := spMsgSubscription{msgChan: msgChan, quitChan: subQuit}
	sp.subscribeRecvMsg(subscription)
	defer func() {
		sp.unsubscribeRecvMsgs(subscription)
		close(subQuit)
	}()

	sp.mempoolScore.Increase(0, filterScore)
	sp.QueueMessage(wire.NewMsgGetCFMempool(wire.GCSFilterRegular), nil)
	r.watchDrop(sp)

	timeout := time.After(r.timeout)
	for {
		select {
		case m := <-msgChan:
			// The filter of the mempool comes with no block hash,
			// which tells it from the filters of blocks.
			cfilter, ok := m.msg.(*wire.MsgCFilter)
			if !ok || cfilter.FilterType != wire.GCSFilterRegular ||
				cfilter.BlockHash != (chainhash.Hash{}) {

				continue
			}
			filter, err := gcs.FromNBytes(builder.DefaultP,
				builder.DefaultM, cfilter.Data)
			if err != nil {
				log.Debugf("Invalid mempool filter from %v: %v",
					sp, err)
				return false
			}
			key := builder.DeriveKey(&chainhash.Hash{})
			matched, err := filter.MatchAny(key, entries)
			if err != nil {
				log.Debugf("Unable to match the mempool filter "+
					"from %v: %v", sp, err)
				return false
			}
			if matched {
				sp.mempoolScore.Increase(0, mempoolScore)
				sp.QueueMessage(wire.NewMsgMemPool(), nil)
				r.watchDrop(sp)
			}
			return true

		case <-timeout:
			return false

		case <-r.quit:
			return false
		}
	}
}

// watchDrop takes sp to have banned us, and asks no peer at its host for the
// mempool again, if it disconnects within dropWindow of the request just sent
// while another peer stays connected.  A network that drops, as a phone's does
// when it switches networks, disconnects every peer, which no ban explains.
func (r *mempoolRequester) watchDrop(sp *ServerPeer) {
	go func() {
		select {
		case <-time.After(r.dropWindow):
		case <-r.quit:
			return
		}
		if sp.Connected() {
			return
		}
		othersConnected := false
		for _, other := range r.peers() {
			if other != nil && other != sp && other.Connected() {
				othersConnected = true
				break
			}
		}
		if !othersConnected {
			return
		}
		log.Warnf("Peer %v disconnected right after we asked it for the "+
			"mempool, so we won't ask it again", sp)
		r.mtx.Lock()
		r.dropped[peerHost(sp)] = struct{}{}
		r.mtx.Unlock()
	}()
}

// droppedUs returns whether a peer at the host of sp disconnected right after
// we asked it for the mempool.
func (r *mempoolRequester) droppedUs(sp *ServerPeer) bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	_, ok := r.dropped[peerHost(sp)]
	return ok
}

// peerHost returns the host of the address of sp.
func peerHost(sp *ServerPeer) string {
	host, _, err := net.SplitHostPort(sp.Addr())
	if err != nil {
		return sp.Addr()
	}
	return host
}

// mempoolBanScores returns what a peer with the user agent adds to its ban
// score for a getcfmempool and a mempool message: bchd's scores from 0.22.5
// on, or the old ones for an earlier version, a pre-release, or a peer whose
// version we can't tell.
func mempoolBanScores(userAgent string) (filter, mempool uint32) {
	const name = "/bchd:"
	i := strings.Index(userAgent, name)
	if i < 0 {
		return oldMempoolBanScore, oldMempoolBanScore
	}
	version := userAgent[i+len(name):]
	if j := strings.IndexAny(version, "/("); j >= 0 {
		version = version[:j]
	}
	fields := strings.Split(version, ".")
	if len(fields) != 3 {
		return oldMempoolBanScore, oldMempoolBanScore
	}
	var v [3]int
	for k, field := range fields {
		n, err := strconv.Atoi(field)
		if err != nil {
			return oldMempoolBanScore, oldMempoolBanScore
		}
		v[k] = n
	}
	if v[0] > 0 || v[1] > 22 || (v[1] == 22 && v[2] >= 5) {
		return mempoolFilterBanScore, mempoolBanScore
	}
	return oldMempoolBanScore, oldMempoolBanScore
}

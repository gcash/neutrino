// NOTE: THIS API IS UNSTABLE RIGHT NOW.

package neutrino

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/gcash/bchd/blockchain"
	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/neutrino/cache"
	"github.com/gcash/neutrino/filterdb"
	"github.com/gcash/neutrino/pushtx"
)

var (
	// QueryTimeout specifies how long to wait for a peer to answer a
	// query.
	QueryTimeout = time.Second * 10

	// QueryBatchTimeout is the total time we'll wait for a batch fetch
	// query to complete.
	QueryBatchTimeout = time.Second * 30

	// QueryPeerCooldown is the time we'll wait before re-assigning a query
	// to a peer that previously failed because of a timeout.
	QueryPeerCooldown = time.Second * 5

	// QueryRejectTimeout is the time we'll wait after sending a response to
	// an INV query for a potential reject answer. If we don't get a reject
	// before this delay, we assume the TX was accepted.
	QueryRejectTimeout = time.Second

	// QueryInvalidTxThreshold is the threshold for the fraction of peers
	// that need to respond to a TX with a code of pushtx.Invalid to count
	// it as invalid, even if not all peers respond. This currently
	// corresponds to 60% of peers that need to reject.
	QueryInvalidTxThreshold float32 = 0.6

	// QueryNumRetries specifies how many times to retry sending a query to
	// each peer before we've concluded we aren't going to get a valid
	// response. This allows to make up for missed messages in some
	// instances.
	QueryNumRetries = 2

	// QueryPeerConnectTimeout specifies how long to wait for the
	// underlying chain service to connect to a peer before giving up
	// on a query in case we don't have any peers.
	QueryPeerConnectTimeout = time.Second * 30

	// QueryEncoding specifies the default encoding (witness or not) for
	// `getdata` and other similar messages.
	QueryEncoding = wire.BaseEncoding
)

// QueryAccess is an interface that gives acces to query a set of peers in
// different ways.
type QueryAccess interface {
	queryAllPeers(
		queryMsg wire.Message,
		checkResponse func(sp *ServerPeer, resp wire.Message,
			quit chan<- struct{}, peerQuit chan<- struct{}),
		options ...QueryOption)
}

// A compile-time check to ensure that ChainService implements the
// QueryAccess interface.
var _ QueryAccess = (*ChainService)(nil)

// maxPeerQueryTimeouts is how many timeouts of a query one turn of a peer can
// use, however much it keeps sending.  A peer gets a turn for each retry.
const maxPeerQueryTimeouts = 10

// maxSilentQueryTries is how many query timeouts a peer can be silent for, not
// sending a byte while a try of a query times out on it, before it's
// disconnected.
const maxSilentQueryTries = 3

// cfilterLock is the filter lock: a one-slot channel, so that a query that can
// give up waits for it and for its caller at once, rather than polling.  Its zero
// value is unlocked.
type cfilterLock struct {
	once sync.Once
	slot chan struct{}
}

// ch returns the lock's slot, which holds a value while it's locked.
func (l *cfilterLock) ch() chan struct{} {
	l.once.Do(func() {
		l.slot = make(chan struct{}, 1)
	})
	return l.slot
}

// Lock waits for the lock and takes it.
func (l *cfilterLock) Lock() {
	l.ch() <- struct{}{}
}

// Unlock frees the lock, which must be held, as a mutex must.
func (l *cfilterLock) Unlock() {
	select {
	case <-l.ch():
	default:
		panic("neutrino: unlock of the unlocked filter lock")
	}
}

// queries are a set of options that can be modified per-query, unlike global
// options.
//
// TODO: Make more query options that override global options.
type queryOptions struct {
	// timeout lets the query know how long to wait for a peer to answer
	// the query before moving onto the next peer.
	timeout time.Duration

	// numRetries tells the query how many times to retry asking each peer
	// the query.
	numRetries uint8

	// peerConnectTimeout lets the query know how long to wait for the
	// underlying chain service to connect to a peer before giving up
	// on a query in case we don't have any peers.
	peerConnectTimeout time.Duration

	// rejectTimeout is the time we'll wait after sending a response to an
	// INV query for a potential reject answer. If we don't get a reject
	// before this delay, we assume the TX was accepted. This option is only
	// used when publishing a transaction.
	rejectTimeout time.Duration

	// encoding lets the query know which encoding to use when queueing
	// messages to a peer.
	encoding wire.MessageEncoding

	// peerLacks tells a message that says the peer doesn't have what was
	// asked for, which ends its turn at once.
	peerLacks func(wire.Message) bool

	// doneChan lets the query signal the caller when it's done, in case
	// it's run in a goroutine.
	doneChan chan<- struct{}

	// quit ends the query early, as a shutdown does.
	quit <-chan struct{}

	// invalidTxThreshold is the threshold for the fraction of peers
	// that need to respond to a TX with a code of pushtx.Invalid to count
	// it as invalid, even if not all peers respond. This option is only
	// used when publishing a transaction.
	invalidTxThreshold float32

	// optimisticBatch indicates whether we expect more calls to follow,
	// and that we should attempt to batch more items with the query such
	// that they can be cached, avoiding the extra round trip.
	optimisticBatch optimisticBatchType
}

// optimisticBatchType is a type indicating the kind of batching we want to
// execute with a query.
type optimisticBatchType uint8

const (
	// noBatch indicates no other than the specified item should be
	// queried.
	noBatch optimisticBatchType = iota

	// forwardBatch is used to indicate we should also query for items
	// following, as they most likely will be fetched next.
	forwardBatch

	// reverseBatch is used to indicate we should also query for items
	// preceding, as they most likely will be fetched next.
	reverseBatch
)

// QueryOption is a functional option argument to any of the network query
// methods, such as GetBlock and GetCFilter (when that resorts to a network
// query). These are always processed in order, with later options overriding
// earlier ones.
type QueryOption func(*queryOptions)

// defaultQueryOptions returns a queryOptions set to package-level defaults.
func defaultQueryOptions() *queryOptions {
	return &queryOptions{
		timeout:            QueryTimeout,
		numRetries:         uint8(QueryNumRetries),
		peerConnectTimeout: QueryPeerConnectTimeout,
		rejectTimeout:      QueryRejectTimeout,
		encoding:           QueryEncoding,
		invalidTxThreshold: QueryInvalidTxThreshold,
		optimisticBatch:    noBatch,
	}
}

// applyQueryOptions updates a queryOptions set with functional options.
func (qo *queryOptions) applyQueryOptions(options ...QueryOption) {
	for _, option := range options {
		option(qo)
	}
}

// Timeout is a query option that lets the query know how long to wait for each
// peer we ask the query to answer it before moving on.
func Timeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.timeout = timeout
	}
}

// NumRetries is a query option that lets the query know the maximum number of
// times each peer should be queried. The default is one.
func NumRetries(numRetries uint8) QueryOption {
	return func(qo *queryOptions) {
		qo.numRetries = numRetries
	}
}

// InvalidTxThreshold is the threshold for the fraction of peers that need to
// respond to a TX with a code of pushtx.Invalid to count it as invalid, even
// if not all peers respond.
//
// NOTE: This option is currently only used when publishing a transaction.
func InvalidTxThreshold(invalidTxThreshold float32) QueryOption {
	return func(qo *queryOptions) {
		qo.invalidTxThreshold = invalidTxThreshold
	}
}

// PeerConnectTimeout is a query option that lets the query know how long to
// wait for the underlying chain service to connect to a peer before giving up
// on a query in case we don't have any peers.
func PeerConnectTimeout(timeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.peerConnectTimeout = timeout
	}
}

// RejectTimeout is the time we'll wait after sending a response to an INV
// query for a potential reject answer. If we don't get a reject before this
// delay, we assume the TX was accepted.
//
// NOTE: This option is currently only used when publishing a transaction.
func RejectTimeout(rejectTimeout time.Duration) QueryOption {
	return func(qo *queryOptions) {
		qo.rejectTimeout = rejectTimeout
	}
}

// Encoding is a query option that allows the caller to set a message encoding
// for the query messages.
func Encoding(encoding wire.MessageEncoding) QueryOption {
	return func(qo *queryOptions) {
		qo.encoding = encoding
	}
}

// peerLacks ends a peer's turn at a message the function tells, rather than
// waiting for a timeout that its other messages keep putting off.
func peerLacks(lacks func(wire.Message) bool) QueryOption {
	return func(qo *queryOptions) {
		qo.peerLacks = lacks
	}
}

// DoneChan allows the caller to pass a channel that will get closed when the
// query is finished.
func DoneChan(doneChan chan<- struct{}) QueryOption {
	return func(qo *queryOptions) {
		qo.doneChan = doneChan
	}
}

// QueryQuit has a query end early when the channel closes, as a shutdown does.
func QueryQuit(quit <-chan struct{}) QueryOption {
	return func(qo *queryOptions) {
		qo.quit = quit
	}
}

// OptimisticBatch allows the caller to tell that items following the requested
// one should be included in the query.
func OptimisticBatch() QueryOption {
	return func(qo *queryOptions) {
		qo.optimisticBatch = forwardBatch
	}
}

// OptimisticReverseBatch allows the caller to tell that items preceding the
// requested one should be included in the query.
func OptimisticReverseBatch() QueryOption {
	return func(qo *queryOptions) {
		qo.optimisticBatch = reverseBatch
	}
}

// queryState is an atomically updated per-query state for each query in a
// batch.
//
// State transitions are:
//
//   - queryWaitSubmit->queryWaitResponse - send query to peer
//   - queryWaitResponse->queryWaitSubmit - query timeout with no acceptable
//     response
//   - queryWaitResponse->queryAnswered - acceptable response to query received
type queryState uint32

const (
	// Waiting to be submitted to a peer.
	queryWaitSubmit queryState = iota

	// Submitted to a peer, waiting for reply.
	queryWaitResponse

	// Valid reply received.
	queryAnswered
)

// We provide 3 kinds of queries:
//
// * queryAllPeers allows a single query to be broadcast to all peers, and
//   then waits for as many peers as possible to answer that query within
//   a timeout. This allows for doing things like checking cfilter checkpoints.
//
// * queryPeers allows a single query to be passed to one peer at a time until
//   the query is deemed answered. This is good for getting a single piece of
//   data, such as a filter or a block.
//
// * queryBatch allows a batch of queries to be distributed among all peers,
//   recirculating upon timeout.
//
// TODO(aakselrod): maybe abstract the query scheduler into a functional option
// and provide some presets (including the ones below) prior to factoring out
// the query API into its own package?

// queryChainServiceBatch is a helper function that sends a batch of queries to
// the entire pool of peers of the given ChainService, attempting to get them
// all answered unless the quit channel is closed. It continues to update its
// view of the connected peers in case peers connect or disconnect during the
// query. The package-level QueryTimeout parameter, overridable by the Timeout
// option, determines how long a peer waits for a query before moving onto the
// next one. The NumRetries option and the QueryNumRetries package-level
// variable are ignored; the query continues until it either completes or the
// passed quit channel is closed.  For memory efficiency, we attempt to get
// responses as close to ordered as we can, so that the caller can cache as few
// responses as possible before committing to storage.
//
// TODO(aakselrod): support for more than one in-flight query per peer to
// reduce effects of latency.
func queryChainServiceBatch(
	// s is the ChainService to use.
	s *ChainService,

	// queryMsgs is a slice of queries for which the caller wants responses.
	queryMsgs []wire.Message,

	// checkResponse is called for every received message to see if it
	// answers the query message. It should return true if so.
	checkResponse func(sp *ServerPeer, query wire.Message, resp wire.Message) bool,

	// queryQuit forces the query to end before it's complete.
	queryQuit <-chan struct{},

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// QueryQuit ends the query as queryQuit does.
	if qo.quit != nil {
		stop := make(chan struct{})
		finished := make(chan struct{})
		defer close(finished)
		go func(queryQuit, quit <-chan struct{}) {
			select {
			case <-queryQuit:
			case <-quit:
			case <-finished:
				return
			}
			close(stop)
		}(queryQuit, qo.quit)
		queryQuit = stop
	}

	// Shared state between this goroutine and the per-peer goroutines.
	queryStates := make([]uint32, len(queryMsgs))

	// subscription allows us to subscribe to notifications from peers.
	msgChan := make(chan spMsg, len(queryMsgs))
	subQuit := make(chan struct{})
	subscription := spMsgSubscription{
		msgChan:  msgChan,
		quitChan: subQuit,
	}
	defer close(subQuit)

	// peerState holds a query message and an answer channel that get a
	// notice when the query got a match. If it's the peer's match, the
	// peer can mark the query a success and move on to the next query
	// ahead of timeout.
	type peerState struct {
		msg         wire.Message
		matchSignal chan struct{}
	}

	// peerStates and its companion mutex allow the peer goroutines to
	// tell the main goroutine what query they're currently working on, and
	// for the main goroutine to signal the peer when a match has been
	// received.
	peerStates := make(map[string]peerState)
	var mtxPeerStates sync.RWMutex

	// allDone is a helper closure we'll use to determine whether we can
	// exit due to all of our queries being answered.
	allDone := func() bool {
		for i := 0; i < len(queryStates); i++ {
			if atomic.LoadUint32(&queryStates[i]) !=
				uint32(queryAnswered) {
				return false
			}
		}
		return true
	}

	// allDoneSignal is a channel we'll close to signal to the main loop
	// that all of the queries have been answered.
	var allDoneOnce sync.Once
	allDoneSignal := make(chan struct{})

	peerGoroutine := func(sp *ServerPeer, quit <-chan struct{},
		allDoneSignal chan struct{}) {

		// Subscribe to messages from the peer.
		sp.subscribeRecvMsg(subscription)
		defer sp.unsubscribeRecvMsgs(subscription)
		defer func() {
			mtxPeerStates.Lock()
			delete(peerStates, sp.Addr())
			mtxPeerStates.Unlock()
		}()

		// Track the last query our peer failed to answer and skip over
		// it for the next attempt. This helps prevent most instances
		// of the same peer being asked for the same query every time.
		firstUnfinished, handleQuery := 0, -1

		for firstUnfinished < len(queryMsgs) {
			select {
			case <-queryQuit:
				return
			case <-s.quit:
				return
			case <-quit:
				return
			default:
			}

			handleQuery = -1

			for i := firstUnfinished; i < len(queryMsgs); i++ {
				// If this query is finished and we're at
				// firstUnfinished, update firstUnfinished.
				if i == firstUnfinished &&
					atomic.LoadUint32(&queryStates[i]) ==
						uint32(queryAnswered) {
					firstUnfinished++

					log.Tracef("Query #%v already answered, "+
						"skipping", i)
					continue
				}

				// We check to see if the query is waiting to
				// be handled. If so, we mark it as being
				// handled. If not, we move to the next one.
				if !atomic.CompareAndSwapUint32(
					&queryStates[i],
					uint32(queryWaitSubmit),
					uint32(queryWaitResponse),
				) {
					log.Tracef("Query #%v already being "+
						"queried for, skipping", i)
					continue
				}

				// The query is now marked as in-process. We
				// begin to process it.
				handleQuery = i
				sp.QueueMessageWithEncoding(queryMsgs[i],
					nil, qo.encoding)
				break
			}

			// Regardless of whether we have a query or not, we
			// need a timeout.
			timeout := time.After(qo.timeout)
			if handleQuery == -1 {
				if firstUnfinished == len(queryMsgs) {
					// We've now answered all the queries.
					return
				}

				// We have nothing to work on but not all
				// queries are answered yet. Wait for a query
				// timeout, or a quit signal, then see if
				// anything needs our help.
				select {
				case <-queryQuit:
					return
				case <-s.quit:
					return
				case <-quit:
					return
				case <-timeout:
					if sp.Connected() {
						continue
					} else {
						return
					}
				}
			}

			// We have a query we're working on.
			matchSignal := make(chan struct{}, 1)
			mtxPeerStates.Lock()
			peerStates[sp.Addr()] = peerState{
				msg:         queryMsgs[handleQuery],
				matchSignal: matchSignal,
			}
			mtxPeerStates.Unlock()

			exiting := false
			select {
			case <-queryQuit:
				exiting = true
			case <-s.quit:
				exiting = true
			case <-quit:
				exiting = true

			case <-timeout:
				// We failed, so set the query state back to
				// zero and update our lastFailed state.
				atomic.StoreUint32(
					&queryStates[handleQuery], uint32(queryWaitSubmit),
				)

				// Delete the query from our peer states, to
				// indicate we are no longer expecting a
				// response.
				mtxPeerStates.Lock()
				delete(peerStates, sp.Addr())
				mtxPeerStates.Unlock()

				log.Tracef("Query for #%v failed, moving "+
					"on: %v", handleQuery,
					newLogClosure(func() string {
						return spew.Sdump(queryMsgs[handleQuery])
					}))

				// To allow other peers to pick up this query,
				// let the peer that just timed out wait a
				// cooldown period before handing it the next
				// query.
				select {
				case <-time.After(QueryPeerCooldown):
				case <-queryQuit:
					return
				case <-s.quit:
					return
				case <-quit:
					return
				}

			case _, ok := <-matchSignal:
				if !ok {
					exiting = true
					break
				}

				log.Tracef("Query #%v answered, updating state",
					handleQuery)

				// We got a match signal so we can mark this
				// query a success.
				atomic.StoreUint32(&queryStates[handleQuery],
					uint32(queryAnswered))

				// If we're done answering all of our queries,
				// we can exit now.
				if allDone() {
					allDoneOnce.Do(func() {
						close(allDoneSignal)
					})
					return
				}
			}

			// Before exiting the peer goroutine, reset the query
			// state to ensure other peers can pick it up.
			if exiting {
				atomic.StoreUint32(
					&queryStates[handleQuery], uint32(queryWaitSubmit),
				)

				// Delete the current state of the peer, so we
				// won't process any lingering responses from
				// it.
				mtxPeerStates.Lock()
				delete(peerStates, sp.Addr())
				mtxPeerStates.Unlock()
				return
			}
		}
	}

	// peerQuits holds per-peer quit channels so we can kill the goroutines
	// when they disconnect.
	peerQuits := make(map[string]chan struct{})

	// Clean up on exit.
	defer func() {
		for _, quitChan := range peerQuits {
			close(quitChan)
		}
	}()

	for {
		// Update our view of peers, starting new workers for new peers
		// and removing disconnected/banned peers.
		for _, peer := range s.Peers() {
			sp := peer.Addr()
			if _, ok := peerQuits[sp]; !ok && peer.Connected() {
				peerQuits[sp] = make(chan struct{})

				go peerGoroutine(
					peer, peerQuits[sp], allDoneSignal,
				)
			}

		}

		for peer, quitChan := range peerQuits {
			p := s.PeerByAddr(peer)
			if p == nil || !p.Connected() {
				close(quitChan)
				delete(peerQuits, peer)
			}
		}

		select {
		case msg := <-msgChan:
			mtxPeerStates.RLock()
			curQuery, ok := peerStates[msg.sp.Addr()]
			mtxPeerStates.RUnlock()

			// Break if we didn't expect a response from this peer.
			if !ok {
				break
			}

			if checkResponse(msg.sp, curQuery.msg, msg.msg) {
				select {
				case <-queryQuit:
					return
				case <-s.quit:
					return
				case curQuery.matchSignal <- struct{}{}:
				}
			}
		case <-time.After(qo.timeout):
		case <-allDoneSignal:
			return
		case <-queryQuit:
			return
		case <-s.quit:
			return
		}
	}
}

// queryAllPeers is a helper function that sends a query to all peers and waits
// for a timeout specified by the QueryTimeout package-level variable or the
// Timeout functional option. The NumRetries option is set to 1 by default
// unless overridden by the caller.
func (s *ChainService) queryAllPeers(
	// queryMsg is the message to broadcast to all peers.
	queryMsg wire.Message,

	// checkResponse is called for every message within the timeout period.
	// The quit channel lets the query know to terminate because the
	// required response has been found. This is done by closing the
	// channel. The peerQuit lets the query know to terminate the query for
	// the peer which sent the response, allowing releasing resources for
	// peers which respond quickly while continuing to wait for slower
	// peers to respond and nonresponsive peers to time out.
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}, peerQuit chan<- struct{}),

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	qo.numRetries = 1
	qo.applyQueryOptions(options...)

	// This is done in a single-threaded query because the peerState is
	// held in a single thread. This is the only part of the query
	// framework that requires access to peerState, so it's done once per
	// query.
	peers := s.Peers()

	// This will be shared state between the per-peer goroutines.
	queryQuit := make(chan struct{})
	allQuit := make(chan struct{})
	var wg sync.WaitGroup
	msgChan := make(chan spMsg)
	subscription := spMsgSubscription{
		msgChan:  msgChan,
		quitChan: allQuit,
	}

	// Now we start a goroutine for each peer which manages the peer's
	// message subscription.
	peerQuits := make(map[string]chan struct{})
	for _, sp := range peers {
		sp.subscribeRecvMsg(subscription)
		wg.Add(1)
		peerQuits[sp.Addr()] = make(chan struct{})
		go func(sp *ServerPeer, peerQuit <-chan struct{}) {
			defer wg.Done()

			defer sp.unsubscribeRecvMsgs(subscription)

			for i := uint8(0); i < qo.numRetries; i++ {
				timeout := time.After(qo.timeout)
				sp.QueueMessageWithEncoding(queryMsg,
					nil, qo.encoding)
				select {
				case <-queryQuit:
					return
				case <-qo.quit:
					return
				case <-s.quit:
					return
				case <-peerQuit:
					return
				case <-timeout:
				}
			}
		}(sp, peerQuits[sp.Addr()])
	}

	// This goroutine will wait until all of the peer-query goroutines have
	// terminated, and then initiate a query shutdown.
	go func() {
		wg.Wait()

		// Make sure our main goroutine and the subscription know to
		// quit.
		close(allQuit)

		// Close the done channel, if any.
		if qo.doneChan != nil {
			close(qo.doneChan)
		}
	}()

	// Loop for any messages sent to us via our subscription channel and
	// check them for whether they satisfy the query. Break the loop when
	// allQuit is closed.
checkResponses:
	for {
		select {
		case <-queryQuit:
			break checkResponses

		case <-s.quit:
			break checkResponses

		case <-allQuit:
			break checkResponses

			// A message has arrived over the subscription channel, so we
			// execute the checkResponses callback to see if this ends our
			// query session.
		case sm := <-msgChan:
			// TODO: This will get stuck if checkResponse gets
			// stuck. This is a caveat for callers that should be
			// fixed before exposing this function for public use.
			select {
			case <-peerQuits[sm.sp.Addr()]:
			default:
				checkResponse(sm.sp, sm.msg, queryQuit,
					peerQuits[sm.sp.Addr()])
			}
		}
	}
}

// queryChainServicePeers is a helper function that sends a query to one or
// more peers of the given ChainService, and waits for an answer. The timeout
// for queries is set by the QueryTimeout package-level variable or the Timeout
// functional option.
func queryChainServicePeers(
	// s is the ChainService to use.
	s *ChainService,

	// queryMsg is the message to send to each peer selected by selectPeer.
	queryMsg wire.Message,

	// checkResponse is called for every message within the timeout period.
	// The quit channel lets the query know to terminate because the
	// required response has been found. This is done by closing the
	// channel.
	checkResponse func(sp *ServerPeer, resp wire.Message,
		quit chan<- struct{}),

	// options takes functional options for executing the query.
	options ...QueryOption) {

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// The sync peer is asked first, then each peer in turn: a peer gets
	// its next try once every other has had as many, so that one that
	// keeps the query for its whole turn doesn't hold it for all its
	// tries before another is asked.
	var queryPeer *ServerPeer
	peerTries := make(map[string]uint8)

	// peerLacked counts the words of each peer that it lacks what was asked,
	// one per try at most: a try that timed out can have its word come in the
	// peer's next try, which only its own word ends.  That's only when its
	// tries come one after another: after another peer's turn, a word on an
	// earlier try came while nothing listened, and can no longer come.
	peerLacked := make(map[string]uint8)
	var lastAsked string

	// This will be state used by the peer query goroutine.
	queryQuit := make(chan struct{})
	subQuit := make(chan struct{})

	// Increase this number to be able to handle more queries at once as
	// each channel gets results for all queries, otherwise messages can
	// get mixed and there's a vicious cycle of retries causing a bigger
	// message flood, more of which get missed.
	msgChan := make(chan spMsg)
	subscription := spMsgSubscription{
		msgChan:  msgChan,
		quitChan: subQuit,
	}

	// Loop for any messages sent to us via our subscription channel and
	// check them for whether they satisfy the query. Break the loop if
	// it's time to quit.
	peerTimeout := time.NewTimer(qo.timeout)
	connectionTimeout := time.NewTimer(qo.peerConnectTimeout)

	// A peer's turn is bounded, as any message it sends resets peerTimeout,
	// whether or not it answers the query, and one that keeps sending
	// others would otherwise keep the query forever.
	turn := newPeerTurn(peerTimeout, qo.timeout)
	defer turn.stop()
	defer connectionTimeout.Stop()

	// That's for a query with no peer to ask, so it must not end one that
	// has peers left to try.
	connectionTicker := connectionTimeout.C

	// ask asks a peer for its next try.
	firstAsked := make(map[string]int)
	var bytesAtAsk uint64
	ask := func(peer *ServerPeer) {
		queryPeer = peer
		bytesAtAsk = peer.bytesIn.Load()
		if _, ok := firstAsked[peer.Addr()]; !ok {
			firstAsked[peer.Addr()] = len(firstAsked)
		}
		if lastAsked != peer.Addr() {
			peerLacked[peer.Addr()] = peerTries[peer.Addr()]
		}
		lastAsked = peer.Addr()
		peerTries[peer.Addr()]++
		peer.subscribeRecvMsg(subscription)
		peer.QueueMessageWithEncoding(queryMsg, nil, qo.encoding)
	}

	// askNextPeer asks the connected peer with tries left that was tried
	// the least, and of those tried as little, the one first asked
	// earliest, or else the first listed, so that each round of tries goes
	// in the order of the first.
	askNextPeer := func() {
		queryPeer = nil
		rank := func(peer *ServerPeer) int {
			if order, ok := firstAsked[peer.Addr()]; ok {
				return order
			}
			return len(firstAsked)
		}
		var next *ServerPeer
		var fewest uint8
		for _, peer := range s.Peers() {
			if !peer.Connected() {
				continue
			}
			numTries := peerTries[peer.Addr()]
			if numTries >= qo.numRetries {
				continue
			}
			if next == nil || numTries < fewest ||
				numTries == fewest && rank(peer) < rank(next) {

				next, fewest = peer, numTries
			}
		}
		if next != nil {
			ask(next)
		}
	}

	// The sync peer goes first without asking for the list of peers,
	// which the peer handler answers, as the query can run where that
	// waits.
	if syncPeer := s.blockManager.SyncPeer(); syncPeer != nil {
		ask(syncPeer)
	} else {
		askNextPeer()
	}
	if queryPeer != nil {
		connectionTicker = nil
	}
checkResponses:
	for {
		select {
		case <-connectionTicker:
			// When we time out, we're done, unless a peer has
			// connected meanwhile.
			askNextPeer()
			if queryPeer == nil {
				break checkResponses
			}
			connectionTicker = nil
			turn.next()

		case <-queryQuit:
			// Same when we get a quit signal.
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		case <-qo.quit:
			// Same when the caller gives up.
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		case <-turn.deadline.C:
			turn.end()

		case <-s.quit:
			// Same when chain server's quit is signaled.
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
			}
			break checkResponses

		// A message has arrived over the subscription channel, so we
		// execute the checkResponses callback to see if this ends our
		// query session.
		case sm := <-msgChan:
			// TODO: This will get stuck if checkResponse gets
			// stuck. This is a caveat for callers that should be
			// fixed before exposing this function for public use.
			checkResponse(sm.sp, sm.msg, queryQuit)

			// Each time we receive a response from the current
			// peer, we'll reset the main peer timeout as they're
			// being responsive, unless their turn is over.  What
			// a peer before it still sends gives it no more time.
			if sm.sp == queryPeer {
				turn.responded()
			}

			// A peer's own word ends its turn. What the last peer
			// said is no word on this one, nor is a word on the
			// peer's earlier try.
			if qo.peerLacks != nil && sm.sp == queryPeer &&
				qo.peerLacks(sm.msg) {

				addr := queryPeer.Addr()
				peerLacked[addr]++
				if peerLacked[addr] >= peerTries[addr] {
					turn.end()
				}
			}

		// The current peer we're querying has failed to answer the
		// query. Time to select a new peer and query it.
		case <-peerTimeout.C:
			if queryPeer != nil {
				queryPeer.unsubscribeRecvMsgs(subscription)
				queryPeer.triedSilently(bytesAtAsk, qo.timeout)
			}

			// The next peer gets a turn of its own.
			turn.next()

			askNextPeer()

			// If at this point, we don't yet have a query peer,
			// then we'll exit now as all the peers are exhausted,
			// unless no peer was there to ask yet, as with a phone
			// that lost reception, which gets its time to connect.
			if queryPeer == nil {
				if connectionTicker != nil {
					continue
				}
				break checkResponses
			}
			connectionTicker = nil
		}
	}

	// Close the subscription quit channel and the done channel, if any.
	close(subQuit)
	peerTimeout.Stop()
	if qo.doneChan != nil {
		close(qo.doneChan)
	}
}

// peerTurn bounds how long a peer has a query.
type peerTurn struct {
	timeout  time.Duration
	idle     *time.Timer
	deadline *time.Timer
	over     bool
}

// newPeerTurn returns the turn of the first peer, with the timer that's reset
// each time the peer sends something.
func newPeerTurn(idle *time.Timer, timeout time.Duration) *peerTurn {
	return &peerTurn{
		timeout:  timeout,
		idle:     idle,
		deadline: time.NewTimer(timeout * maxPeerQueryTimeouts),
	}
}

// responded gives the peer another timeout, unless its turn is over.
func (t *peerTurn) responded() {
	if !t.over {
		resetTimer(t.idle, t.timeout)
	}
}

// end ends the peer's turn, whatever it's sending, as the idle timer firing does.
func (t *peerTurn) end() {
	t.over = true
	resetTimer(t.idle, 0)
}

// next starts the next peer's turn, which has a timeout of its own, as the idle
// timer has fired and only a message would start it again.
func (t *peerTurn) next() {
	t.over = false
	resetTimer(t.idle, t.timeout)
	resetTimer(t.deadline, t.timeout*maxPeerQueryTimeouts)
}

func (t *peerTurn) stop() {
	t.deadline.Stop()
}

// resetTimer sets a timer, whether or not it has fired.
func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

// lockCFilter takes the lock on filter queries, and returns false if the query's
// caller gave up first.  The query that has it can take a while with a peer that
// doesn't answer, and a sync.Mutex can't be waited on along with a channel.
func (s *ChainService) lockCFilter(options ...QueryOption) bool {
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	if qo.quit == nil {
		s.mtxCFilter.Lock()
		return true
	}
	select {
	case s.mtxCFilter.ch() <- struct{}{}:
		return true
	case <-qo.quit:
		return false
	case <-s.quit:
		return false
	}
}

// queryEnded returns whether a query's caller gave up or we're shutting down.
func (s *ChainService) queryEnded(options ...QueryOption) bool {
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	select {
	case <-qo.quit:
		return true
	case <-s.quit:
		return true
	default:
		return false
	}
}

// getFilterFromCache returns a filter from ChainService's FilterCache if it
// exists, returning nil and error if it doesn't.
func (s *ChainService) getFilterFromCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType) (*gcs.Filter, error) {

	cacheKey := cache.FilterCacheKey{BlockHash: *blockHash, FilterType: filterType}

	filterValue, err := s.FilterCache.Get(cacheKey)
	if err != nil {
		return nil, err
	}

	return filterValue.(*cache.CacheableFilter).Filter, nil
}

// putFilterToCache inserts a given filter in ChainService's FilterCache.
func (s *ChainService) putFilterToCache(blockHash *chainhash.Hash,
	filterType filterdb.FilterType, filter *gcs.Filter) (bool, error) {

	cacheKey := cache.FilterCacheKey{BlockHash: *blockHash, FilterType: filterType}
	return s.FilterCache.Put(cacheKey, &cache.CacheableFilter{Filter: filter})
}

// cfiltersQuery is a struct that holds all the information necessary to
// perform batch GetCFilters request, and handle the responses.
type cfiltersQuery struct {
	filterType    wire.FilterType
	startHeight   int64
	stopHeight    int64
	stopHash      *chainhash.Hash
	filterHeaders []chainhash.Hash
	headerIndex   map[chainhash.Hash]int
	targetHash    chainhash.Hash
	filterChan    chan *gcs.Filter
	options       []QueryOption

	// mismatched holds the blocks a peer served a filter for that doesn't
	// match the filter header we synced for it.
	mismatched map[chainhash.Hash]struct{}

	// gen is the count of rollbacks of the filter headers before we read
	// them, and stale is set once they're rolled back again, which ends
	// the query.
	gen   uint64
	stale bool
}

// queryMsg returns the wire message to perform this query.
func (q *cfiltersQuery) queryMsg() wire.Message {
	return wire.NewMsgGetCFilters(
		q.filterType, uint32(q.startHeight), q.stopHash,
	)
}

// prepareCFiltersQuery creates a cfiltersQuery that can be used to fetch a
// CFilter fo the given block hash.
func (s *ChainService) prepareCFiltersQuery(blockHash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (
	*cfiltersQuery, error) {

	// We'll note the count of rollbacks of the filter headers before we
	// read them, so we can tell whether they're rolled back again before
	// a filter we check against them arrives.
	s.filterGenMtx.Lock()
	gen := s.filterGen
	s.filterGenMtx.Unlock()

	_, height, err := s.BlockHeaders.FetchHeader(&blockHash)
	if err != nil {
		return nil, fmt.Errorf("unable to get header for start "+
			"block=%v: %v", blockHash, err)
	}

	bestBlock, err := s.BestBlock()
	if err != nil {
		return nil, fmt.Errorf("unable to get best block: %v", err)
	}
	bestHeight := int64(bestBlock.Height)

	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)

	// If the query specifies an optimistic batch we will attempt to fetch
	// the maximum number of filters in anticipation of calls for the
	// following or preceding filters.
	var startHeight, stopHeight int64

	switch qo.optimisticBatch {

	// No batching, the start and stop height will be the same.
	case noBatch:
		startHeight = int64(height)
		stopHeight = int64(height)

	// Forward batch, fetch as many of the following filters as possible.
	case forwardBatch:
		startHeight = int64(height)
		stopHeight = startHeight + wire.MaxGetCFiltersReqRange - 1

		// We need a longer timeout, since we are going to receive more
		// than a single response.
		options = append(options, Timeout(QueryBatchTimeout))

	// Reverse batch, fetch as many of the preceding filters as possible.
	case reverseBatch:
		stopHeight = int64(height)
		startHeight = stopHeight - wire.MaxGetCFiltersReqRange + 1

		// We need a longer timeout, since we are going to receive more
		// than a single response.
		options = append(options, Timeout(QueryBatchTimeout))
	}

	// Block 1 is the earliest one we can fetch.
	if startHeight < 1 {
		startHeight = 1
	}

	// If the stop height with the maximum batch size is above our best
	// known block, then we use the best block height instead.
	if stopHeight > bestHeight {
		stopHeight = bestHeight
	}

	stopHash, err := s.GetBlockHash(stopHeight)
	if err != nil {
		return nil, fmt.Errorf("unable to get hash for "+
			"stopHeight=%d: %v", stopHeight, err)
	}

	// In order to verify the authenticity of the received filters, we'll
	// fetch the block headers and filter headers in the range
	// [startHeight-1, stopHeight]. We go one below our startHeight since
	// the hash of the previous block is needed for validation.
	numFilters := uint32(stopHeight - startHeight + 1)
	blockHeaders, _, err := s.BlockHeaders.FetchHeaderAncestors(
		numFilters, stopHash,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get %d block header "+
			"ancestors for stopHash=%v: %v", numFilters,
			stopHash, err)
	}

	if len(blockHeaders) != int(numFilters)+1 {
		return nil, fmt.Errorf("expected %d block headers, got %d",
			numFilters+1, len(blockHeaders))
	}

	filterHeaders, _, err := s.filterHeaders.FetchHeaderAncestors(
		numFilters, stopHash,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get %d filter header "+
			"ancestors for stopHash=%v: %v", numFilters, stopHash,
			err)
	}

	if len(filterHeaders) != int(numFilters)+1 {
		return nil, fmt.Errorf("expected %d filter headers, got %d",
			numFilters+1, len(filterHeaders))
	}

	// We create a header index such that we can easily index into our
	// header slices for a given block hash in the received response,
	// without consulting the database. This also keeps track of which
	// blocks we are still awaiting a response for. We start at index=1, as
	// 0 is for the block startHeight-1, which is only needed for
	// validation.
	headerIndex := make(map[chainhash.Hash]int, len(blockHeaders)-1)
	for i := 1; i < len(blockHeaders); i++ {
		block := blockHeaders[i]
		headerIndex[block.BlockHash()] = i
	}

	// We'll immediately respond to the caller with the requested filter
	// when it is received, so we make a channel to notify on when it's
	// ready.
	filterChan := make(chan *gcs.Filter, 1)

	return &cfiltersQuery{
		filterType:    filterType,
		startHeight:   startHeight,
		stopHeight:    stopHeight,
		stopHash:      stopHash,
		filterHeaders: filterHeaders,
		headerIndex:   headerIndex,
		targetHash:    blockHash,
		filterChan:    filterChan,
		options:       options,
		mismatched:    make(map[chainhash.Hash]struct{}),
		gen:           gen,
	}, nil
}

// handleCFiltersRespons is called every time we receive a response for the
// GetCFilters request.
func (s *ChainService) handleCFiltersResponse(q *cfiltersQuery,
	resp wire.Message, quit chan<- struct{}) {

	// The query is over once the filter headers it read are rolled back.
	if q.stale {
		return
	}

	// We're only interested in "cfilter" messages.
	response, ok := resp.(*wire.MsgCFilter)
	if !ok {
		return
	}

	// If the response doesn't match our request, ignore this message.
	if q.filterType != response.FilterType {
		return
	}

	// If this filter is for a block not in our index, we can ignore it, as
	// we either already got it, or it is out of our queried range.
	i, ok := q.headerIndex[response.BlockHash]
	if !ok {
		return
	}

	// We'll only check, return and keep the filter while the filter
	// headers we read stand, which holding the lock that rollbacks of them
	// hold makes sure of. Otherwise the query is over, and the caller can
	// fetch the filter again.
	s.filterGenMtx.Lock()
	defer s.filterGenMtx.Unlock()
	if s.filterGen != q.gen {
		q.stale = true
		close(quit)
		return
	}

	gotFilter, root, err := parseServedFilter(q.filterType, response.Data)
	if err != nil {
		// Malformed filter data. We can ignore this message.
		return
	}

	// Now that we have a proper filter, ensure that re-calculating the
	// filter header hash for the header _after_ the filter in the chain
	// checks out. If not, we can ignore this response.
	curHeader := q.filterHeaders[i]
	prevHeader := q.filterHeaders[i-1]
	gotHeader, err := servedFilterHeader(
		q.filterType, gotFilter, root, prevHeader,
	)
	if err != nil {
		return
	}

	// Either the peer or the filter header we synced is wrong, which a
	// repair of the filter headers can settle if no peer serves a filter
	// that matches.
	if gotHeader != curHeader {
		q.mismatched[response.BlockHash] = struct{}{}
		return
	}

	// At this point, the filter matches what we know about it and we
	// declare it sane. If this is the filter requested initially, send it
	// to the caller immediately.
	if response.BlockHash == q.targetHash {
		q.filterChan <- gotFilter
	}

	// Put the filter in the cache and persist it to disk if
	// Config.PersistToDisk is set.
	// TODO(halseth): for an LRU we could take care to insert the next
	// height filter last.
	dbFilterType := dbFilterTypeOf(q.filterType)
	evict, err := s.putFilterToCache(
		&response.BlockHash, dbFilterType, gotFilter,
	)
	if err != nil {
		log.Warnf("Couldn't write filter to cache: %v", err)
	}

	// TODO(halseth): dynamically increase/decrease the batch size to match
	// our cache capacity.
	numFilters := q.stopHeight - q.startHeight + 1
	if evict && s.FilterCache.Len() < int(numFilters) {
		log.Debugf("Items evicted from the cache with less "+
			"than %d elements. Consider increasing the "+
			"cache size...", numFilters)
	}

	if s.persistToDisk {
		err = s.FilterDB.PutFilter(
			&response.BlockHash, gotFilter, dbFilterType,
		)
		if err != nil {
			log.Warnf("Couldn't write filter to filterDB: "+
				"%v", err)
		}

		log.Tracef("Wrote filter for block %s, type %d",
			&response.BlockHash, dbFilterType)
	}

	// Finally, we can delete it from the headerIndex.
	delete(q.headerIndex, response.BlockHash)

	// If the headerIndex is empty, we got everything we wanted, and can
	// exit.
	if len(q.headerIndex) == 0 {
		close(quit)
	}
}

// GetCFilter gets a cfilter from the database. Failing that, it requests the
// cfilter from the network and, if Config.PersistToDisk is set, writes it to
// the database. Only the regular filter type is supported.
func (s *ChainService) GetCFilter(blockHash chainhash.Hash,
	filterType wire.FilterType, options ...QueryOption) (*gcs.Filter, error) {

	// We only have the filter headers of the type we sync to check a
	// filter against, so we'll reject all other filters.
	if filterType != s.filterType {
		return nil, fmt.Errorf("filter type %v is not the one synced, %v",
			filterType, s.filterType)
	}

	// The filter type tells the cache and the database apart.
	dbFilterType := dbFilterTypeOf(filterType)

	// First check the cache to see if we already have this filter. If
	// so, then we can return it an exit early.
	filter, err := s.getFilterFromCache(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		return filter, nil
	}
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}

	// If not in cache, check if it's in database, returning early if yes.
	filter, err = s.FilterDB.FetchFilter(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		return filter, nil
	}
	if err != nil && err != filterdb.ErrFilterNotFound {
		return nil, err
	}

	// We acquire the mutex ensuring we don't have several redundant
	// CFilter queries running in parallel.
	if !s.lockCFilter(options...) {
		select {
		case <-s.quit:
			return nil, ErrShuttingDown
		default:
		}

		return nil, ErrFilterFetchFailed
	}

	// Since another request might have added the filter to the cache while
	// we were waiting for the mutex, we do a final lookup before starting
	// our own query.
	filter, err = s.getFilterFromCache(&blockHash, dbFilterType)
	if err == nil && filter != nil {
		s.mtxCFilter.Unlock()
		return filter, nil
	}
	if err != nil && err != cache.ErrElementNotFound {
		s.mtxCFilter.Unlock()
		return nil, err
	}

	// We didn't get the filter from the DB, so we'll try to get it from
	// the network.
	query, err := s.prepareCFiltersQuery(blockHash, filterType, options...)
	if err != nil {
		s.mtxCFilter.Unlock()
		return nil, err
	}

	// With all the necessary items retrieved, we'll launch our concurrent
	// query to the set of connected peers.
	log.Debugf("Fetching filters for heights=[%v, %v], stophash=%v",
		query.startHeight, query.stopHeight, query.stopHash)

	go func() {
		defer s.mtxCFilter.Unlock()
		defer close(query.filterChan)

		s.queryPeers(
			// Send a wire.MsgGetCFilters
			query.queryMsg(),

			// Check responses and if we get one that matches, end
			// the query early.
			func(_ *ServerPeer, resp wire.Message, quit chan<- struct{}) {
				s.handleCFiltersResponse(query, resp, quit)
			},
			query.options...,
		)

		// If there are elements left to receive, the query failed.
		if len(query.headerIndex) > 0 {
			numFilters := query.stopHeight - query.startHeight + 1
			log.Errorf("Query failed with %d out of %d filters "+
				"received", len(query.headerIndex), numFilters)

			// If the filter headers were rolled back meanwhile,
			// the query ended without checking every filter
			// served against the filter headers that stand. And
			// a query that was ended didn't ask every peer.
			if query.stale || s.queryEnded(query.options...) {
				return
			}

			// If the only filters served for a block we're missing
			// don't match its filter header, the filter headers
			// may be wrong, so we'll have them repaired.
			for hash := range query.headerIndex {
				if _, ok := query.mismatched[hash]; ok {
					log.Warnf("Filters served for block %v "+
						"don't match its filter header",
						hash)
					s.blockManager.requestFilterHeaderRepair()
					break
				}
			}

			return
		}
	}()

	var ok bool
	select {

	// We'll return immediately to the caller when the filter arrives.
	case filter, ok = <-query.filterChan:
		if !ok {
			// The query ended without the filter, either because
			// we're shutting down or because no peer sent it.
			select {
			case <-s.quit:
				return nil, ErrShuttingDown
			default:
			}

			return nil, ErrFilterFetchFailed
		}

		return filter, nil

	case <-s.quit:
		return nil, ErrShuttingDown
	}
}

// GetBlock gets a block by requesting it from the network, one peer at a
// time, until one answers. If the block is found in the cache, it will be
// returned immediately.
func (s *ChainService) GetBlock(blockHash chainhash.Hash,
	options ...QueryOption) (*bchutil.Block, error) {

	// Fetch the corresponding block header from the database. If this
	// isn't found, then we don't have the header for this block so we
	// can't request it.
	blockHeader, height, err := s.BlockHeaders.FetchHeader(&blockHash)
	if err != nil || blockHeader.BlockHash() != blockHash {
		return nil, fmt.Errorf("couldn't get header for block %s "+
			"from database", blockHash)
	}

	// Starting with the set of default options, we'll apply any specified
	// functional options to the query so that we can check what inv type
	// to use.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	invType := wire.InvTypeBlock
	if qo.encoding == wire.BaseEncoding {
		invType = wire.InvTypeBlock
	}

	// Create an inv vector for getting this block.
	inv := wire.NewInvVect(invType, &blockHash)

	// If the block is already in the cache, we can return it immediately.
	blockValue, err := s.BlockCache.Get(*inv)
	if err == nil && blockValue != nil {
		return blockValue.(*cache.CacheableBlock).Block, err
	}
	if err != nil && err != cache.ErrElementNotFound {
		return nil, err
	}

	foundBlock, err := s.fetchBlock(inv, height, options...)
	if err != nil {
		return nil, err
	}

	// Check the block against its filter header, since filters are only
	// as good as the filter headers synced for them.
	if err := s.verifyBlockFilter(foundBlock, height); err != nil {
		return nil, err
	}

	// Add block to the cache before returning it.
	_, err = s.BlockCache.Put(*inv, &cache.CacheableBlock{Block: foundBlock})
	if err != nil {
		log.Warnf("couldn't write block to cache: %v", err)
	}

	return foundBlock, nil
}

// notFoundOf returns whether a message says the peer doesn't have what the inv
// vector names.
func notFoundOf(msg wire.Message, inv *wire.InvVect) bool {
	notFound, ok := msg.(*wire.MsgNotFound)
	if !ok {
		return false
	}
	for _, item := range notFound.InvList {
		if item.Hash == inv.Hash {
			return true
		}
	}
	return false
}

// fetchBlock requests the block with the given inv vector and height from the
// network, one peer at a time, until one answers with it. Unlike GetBlock, it
// neither checks the block against its filter header nor caches it, so it can
// be used to check the filter headers themselves.
func (s *ChainService) fetchBlock(inv *wire.InvVect, height uint32,
	options ...QueryOption) (*bchutil.Block, error) {

	// A block comes as one message, so nothing resets a peer's timeout
	// while it does, and it can take a slow link longer than a small
	// answer.  A caller's timeout still comes first.
	options = append([]QueryOption{Timeout(QueryBatchTimeout)}, options...)

	// We have this block's header, so a peer that answers it hasn't got the
	// block can only be missing it, and another peer may have it.
	options = append(options, peerLacks(func(msg wire.Message) bool {
		return notFoundOf(msg, inv)
	}))

	blockHash := inv.Hash

	// Construct the appropriate getdata message to fetch the target block.
	getData := wire.NewMsgGetData()
	_ = getData.AddInvVect(inv)

	// The block is only updated from the checkResponse function argument,
	// which is always called single-threadedly. We don't check the block
	// until after the query is finished, so we can just write to it
	// naively.
	var foundBlock *bchutil.Block
	s.queryPeers(
		// Send a wire.GetDataMsg
		getData,

		// Check responses and if we get one that matches, end the
		// query early.
		func(sp *ServerPeer, resp wire.Message,
			quit chan<- struct{}) {
			switch response := resp.(type) {
			// We're only interested in "block" messages.
			case *wire.MsgBlock:
				// Only keep this going if we haven't already
				// found a block, or we risk closing an already
				// closed channel.
				if foundBlock != nil {
					return
				}

				// If this isn't our block, ignore it.
				if response.BlockHash() != blockHash {
					return
				}
				block := bchutil.NewBlock(response)

				// Only set height if bchutil hasn't
				// automagically put one in.
				if block.Height() == bchutil.BlockHeightUnknown {
					block.SetHeight(int32(height))
				}

				// If this claims our block but doesn't pass
				// the sanity check, the peer is trying to
				// bamboozle us. Disconnect it.
				if err := blockchain.CheckBlockSanity(
					block,
					// We don't need to check PoW because
					// by the time we get here, it's been
					// checked during header
					// synchronization
					s.chainParams.PowLimit,
					s.timeSource,
					int32(height) > s.chainParams.MagneticAnonomalyForkHeight,
					int32(height) > s.chainParams.Upgrade9ForkHeight,
				); err != nil {
					log.Warnf("Invalid block for %s "+
						"received from %s -- "+
						"disconnecting peer", blockHash,
						sp.Addr())
					sp.Disconnect()
					return
				}

				// TODO(roasbeef): modify CheckBlockSanity to
				// also check witness commitment

				// At this point, the block matches what we
				// know about it and we declare it sane. We can
				// kill the query and pass the response back to
				// the caller.
				foundBlock = block
				close(quit)
			default:
			}
		},
		options...,
	)
	if foundBlock == nil {
		select {
		case <-s.quit:
			return nil, ErrShuttingDown
		default:
		}

		return nil, fmt.Errorf("couldn't retrieve block %s from network", blockHash)
	}

	return foundBlock, nil
}

// sendTransaction sends a transaction to all peers. It returns an error if any
// peer rejects the transaction.
//
// TODO: Better privacy by sending to only one random peer and watching
// propagation, requires better peer selection support in query API.
//
// TODO(wilmer): Move to pushtx package after introducing a query package. This
// cannot be done at the moment due to circular dependencies.
func (s *ChainService) sendTransaction(tx *wire.MsgTx, options ...QueryOption) error {
	// Starting with the set of default options, we'll apply any specified
	// functional options to the query so that we can check what inv type
	// to use. Broadcast the inv to all peers, responding to any getdata
	// messages for the transaction.
	qo := defaultQueryOptions()
	qo.applyQueryOptions(options...)
	invType := wire.InvTypeBlock
	if qo.encoding == wire.BaseEncoding {
		invType = wire.InvTypeTx
	}

	// Create an inv.
	txHash := tx.TxHash()
	inv := wire.NewMsgInv()
	_ = inv.AddInvVect(wire.NewInvVect(invType, &txHash))

	// We'll gather all the peers who replied to our query, along with
	// the ones who rejected it and their reason for rejecting it. We'll use
	// this to determine whether our transaction was actually rejected.
	replies := make(map[int32]struct{})
	rejections := make(map[int32]*pushtx.BroadcastError)
	rejectCodes := make(map[pushtx.BroadcastErrorCode]int)

	// closers is a map that tracks the delayed closers we need to make sure
	// the peer quit channel is closed after a timeout.
	closers := make(map[int32]*delayedCloser)

	// Send the peer query and listen for getdata.
	s.queryAllPeers(
		inv,
		func(sp *ServerPeer, resp wire.Message, _ chan<- struct{},
			peerQuit chan<- struct{}) {

			// The "closer" can be used to either close the peer
			// quit channel after a certain timeout or immediately.
			closer, ok := closers[sp.ID()]
			if !ok {
				closer = newDelayedCloser(
					peerQuit, qo.rejectTimeout,
				)
				closers[sp.ID()] = closer
			}

			switch response := resp.(type) {
			// A peer has replied with a GetData message, so we'll
			// send them the transaction.
			case *wire.MsgGetData:
				for _, vec := range response.InvList {
					if vec.Hash == txHash {
						sp.QueueMessageWithEncoding(
							tx, nil, qo.encoding,
						)

						// Peers might send the INV
						// request multiple times, we
						// need to de-duplicate them
						// using a map.
						replies[sp.ID()] = struct{}{}

						// Okay, so the peer responded
						// with an INV message, and we
						// sent out the TX. If we never
						// hear anything back from the
						// peer it means they accepted
						// the tx. If we get a reject,
						// things are clear as well. But
						// what if they are just slow to
						// respond? We'll give them
						// another bit of time to
						// respond.
						closer.closeEventually(s.quit)
					}
				}

				// A peer has rejected our transaction for whatever
				// reason. Rather than returning to the caller upon the
				// first rejection, we'll gather them all to determine
				// whether it is critical/fatal.
			case *wire.MsgReject:
				// Ensure this rejection is for the transaction
				// we're attempting to broadcast.
				if response.Hash != txHash {
					return
				}

				broadcastErr := pushtx.ParseBroadcastError(
					response, sp.Addr(),
				)
				rejections[sp.ID()] = broadcastErr
				rejectCodes[broadcastErr.Code]++

				log.Debugf("Transaction %v rejected by peer "+
					"%v: code = %v, reason = %q", txHash,
					sp.Addr(), broadcastErr.Code,
					broadcastErr.Reason)

				// A reject message is final, so we can close
				// the peer quit channel now, we don't expect
				// any further messages.
				closer.closeNow()
			}
		},
		// Listen for replies for the duration of the configured
		// broadcast timeout, see Config.BroadcastTimeout. Default for
		// queryAllPeers is a single try.
		append(
			[]QueryOption{Timeout(s.broadcastTimeout)},
			options...,
		)...,
	)

	// If none of our peers replied to our query, we'll avoid returning an
	// error as the reliable broadcaster will take care of broadcasting this
	// transaction upon every block connected/disconnected.
	if len(replies) == 0 {
		log.Debugf("No peers replied to inv message for transaction %v",
			txHash)
		return nil
	}

	// firstRejectWithCode returns the first reject error that we have for
	// a certain error code.
	firstRejectWithCode := func(code pushtx.BroadcastErrorCode) error {
		for _, broadcastErr := range rejections {
			if broadcastErr.Code == code {
				return broadcastErr
			}
		}

		// We can't really get here unless something is totally wrong in
		// the above error mapping code.
		return fmt.Errorf("invalid error mapping")
	}

	// If all of our peers who replied to our query also rejected our
	// transaction, we'll deem that there was actually something wrong with
	// it, so we'll return the most rejected error between all of our peers.
	log.Debugf("Got replies from %d peers and %d rejections", len(replies),
		len(rejections))
	if len(replies) == len(rejections) {
		log.Warnf("All peers rejected transaction %v checking errors",
			txHash)

		// First, find the reject code that was returned most often.
		var (
			mostRejectedCount = 0
			mostRejectedCode  pushtx.BroadcastErrorCode
		)
		for code, count := range rejectCodes {
			if count > mostRejectedCount {
				mostRejectedCount = count
				mostRejectedCode = code
			}
		}

		// Then return the first error we have for that code (it doesn't
		// really matter which one, as long as our error code parsing is
		// correct).
		return firstRejectWithCode(mostRejectedCode)
	}

	// We did get some rejections, but not from all peers. Perhaps that's
	// due to some peers responding too slowly. Or it could also be a
	// malicious peer trying to block us from publishing a TX. That's why
	// we want to check if more than 60% of the peers (by default) that
	// replied in time also sent a reject, we can be pretty certain that
	// this TX is probably invalid.
	if len(rejections) > 0 {
		numInvalid := rejectCodes[pushtx.Invalid]
		numPeersResponded := len(replies)

		log.Debugf("Of %d peers that replied, %d think the TX is "+
			"invalid", numPeersResponded, numInvalid)

		// 60% or more (by default) of the peers declared this TX as
		// invalid.
		if float32(numInvalid)/float32(numPeersResponded) >=
			qo.invalidTxThreshold {

			log.Warnf("Threshold of %v reached (%d out of %d "+
				"peers), declaring TX %v as invalid",
				qo.invalidTxThreshold, numInvalid,
				numPeersResponded, txHash)

			return firstRejectWithCode(pushtx.Invalid)
		}
	}

	return nil
}

// delayedCloser is a struct that makes sure a subject channel is closed at some
// point, either after a delay or immediately.
type delayedCloser struct {
	subject chan<- struct{}
	timeout time.Duration
	once    sync.Once
}

// newDelayedCloser creates a new delayed closer for the given subject channel.
func newDelayedCloser(subject chan<- struct{},
	timeout time.Duration) *delayedCloser {

	return &delayedCloser{
		subject: subject,
		timeout: timeout,
	}
}

// closeEventually closes the subject channel after the configured timeout or
// immediately if the quit channel is closed.
func (t *delayedCloser) closeEventually(quit chan struct{}) {
	go func() {
		defer t.closeNow()

		select {
		case <-time.After(t.timeout):
		case <-quit:
		}
	}()
}

// closeNow immediately closes the subject channel. This can safely be called
// multiple times as it will only attempt to close the channel at most once.
func (t *delayedCloser) closeNow() {
	t.once.Do(func() {
		close(t.subject)
	})
}

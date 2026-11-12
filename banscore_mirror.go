package neutrino

import (
	"sync"

	"github.com/gcash/bchd/connmgr"
)

// banScoreMirror mirrors the ban score a peer keeps for us.  The check that a
// request still fits and the add that counts it happen together, so two
// senders cannot each see room and together pass the score the peer bans at.
type banScoreMirror struct {
	mtx   sync.Mutex
	score connmgr.DynamicBanScore
}

// Int returns the score as it is now.
func (m *banScoreMirror) Int() uint32 {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.score.Int()
}

// tryTake adds add to the score when the score plus room is at max or below,
// and reports whether it did.  room is at least add: a sender that will send
// a second request after this one passes the scores of both as room, so that
// it only starts what it can finish.
func (m *banScoreMirror) tryTake(add, room, max uint32) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.score.Int()+room > max {
		return false
	}
	m.score.Increase(0, add)
	return true
}

// String returns the score the way DynamicBanScore does, for logs.
func (m *banScoreMirror) String() string {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.score.String()
}

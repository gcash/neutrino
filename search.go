package neutrino

import (
	"errors"

	"github.com/gcash/bchd/chaincfg/chainhash"
	"github.com/gcash/bchd/wire"
	"github.com/gcash/bchutil"
	"github.com/gcash/bchutil/gcs"
	"github.com/gcash/bchutil/gcs/builder"
)

// ErrSearchCancelled is returned by a search that was told to stop.
var ErrSearchCancelled = errors.New("search cancelled")

// ErrSpendSearchCancelled is ErrSearchCancelled, by the name it had when only
// SearchSpends returned it.
//
// Deprecated: use ErrSearchCancelled.
var ErrSpendSearchCancelled = ErrSearchCancelled

// searchSource is what a search reads.  ChainService implements it.
type searchSource interface {
	GetBlockHash(height int64) (*chainhash.Hash, error)
	GetCFilter(hash chainhash.Hash, filterType wire.FilterType,
		options ...QueryOption) (*gcs.Filter, error)
	GetBlock(hash chainhash.Hash, options ...QueryOption) (*bchutil.Block, error)
}

// scanStep is what a search does after a block that its filter matched.
type scanStep int

const (
	// scanOn goes on to the next height.
	scanOn scanStep = iota

	// scanFinished ends the search, with the block's height done, as there
	// is nothing left to look for.
	scanFinished

	// scanStopped ends the search at the block's height, which isn't done.
	scanStopped
)

// scanFilters reads the filters of the blocks from height from to height to,
// both included, in that direction: up, or down when from is above to.  It
// hands each block whose filter matches the entries to onBlock, which returns
// what to do next and whether the entries changed, when they are asked for
// again.  It ends at once if there are none.
//
// progress, if it isn't nil, is called with each height that is done.  The
// height the search got to is returned, with an error too: the last done, or
// from less one step if none was, or the height of a block where onBlock
// stopped it.  A search that quit closes, or that shutdown does, returns
// ErrSearchCancelled.
func scanFilters(source searchSource, from, to int32, entries func() ([][]byte, error),
	onBlock func(block *wire.MsgBlock, hash *chainhash.Hash, height int32) (scanStep, bool),
	progress func(height int32), quit, shutdown <-chan struct{}) (int32, error) {

	step, batch := int32(1), OptimisticBatch()
	if from > to {
		step, batch = -1, OptimisticReverseBatch()
	}
	reached := from - step
	current, err := entries()
	if err != nil || len(current) == 0 {
		return reached, err
	}
	for height := from; height != to+step; height += step {
		select {
		case <-quit:
			return reached, ErrSearchCancelled
		case <-shutdown:
			return reached, ErrSearchCancelled
		default:
		}

		hash, err := source.GetBlockHash(int64(height))
		if err != nil {
			return reached, err
		}
		matched, err := filterMatches(source, hash, current, batch, QueryQuit(quit))
		if err != nil {
			return reached, stopped(err, quit, shutdown)
		}
		next := scanOn
		if matched {
			block, err := source.GetBlock(*hash, QueryQuit(quit))
			if err != nil {
				return reached, stopped(err, quit, shutdown)
			}
			if block == nil {
				return reached, errors.New("no block for a filter that matched")
			}
			var changed bool
			next, changed = onBlock(block.MsgBlock(), hash, height)
			if next == scanStopped {
				return height, nil
			}
			if changed {
				current, err = entries()
				if err != nil {
					return reached, err
				}
			}
		}
		reached = height
		if progress != nil {
			progress(height)
		}
		if next == scanFinished {
			return reached, nil
		}
	}
	return reached, nil
}

// stopped returns ErrSearchCancelled for an error that ending the search
// brought, as a query that's told to quit just ends without what it asked for.
func stopped(err error, quit, shutdown <-chan struct{}) error {
	select {
	case <-quit:
		return ErrSearchCancelled
	case <-shutdown:
		return ErrSearchCancelled
	default:
		return err
	}
}

// filterMatches returns whether the filter of the block matches any of the
// entries.  It only ever tells that a block may hold one.
func filterMatches(source searchSource, hash *chainhash.Hash, entries [][]byte,
	options ...QueryOption) (bool, error) {

	filter, err := source.GetCFilter(*hash, filterTypeOf(source), options...)
	if err != nil {
		return false, err
	}
	if filter == nil {
		return false, ErrFilterFetchFailed
	}
	if filter.N() == 0 {
		return false, nil
	}
	return filter.MatchAny(builder.DeriveKey(hash), entries)
}

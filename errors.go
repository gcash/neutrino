package neutrino

import "errors"

var (
	// ErrGetUtxoCancelled signals that a GetUtxo request was cancelled.
	ErrGetUtxoCancelled = errors.New("get utxo request cancelled")

	// ErrShuttingDown signals that neutrino received a shutdown request.
	ErrShuttingDown = errors.New("neutrino shutting down")

	// ErrFilterFetchFailed signals that no peer sent a compact filter that
	// was asked for.
	ErrFilterFetchFailed = errors.New("unable to fetch cfilter")
)

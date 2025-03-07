package cardinality

import "io"

// Estimator describes cardinality estimation counter interface.
type Estimator interface {
	io.ReaderFrom
	io.WriterTo
	// Add adds new key to the counter.
	Add(key any) error
	// HAdd adds new precalculated hash key to the counter.
	HAdd(hkey uint64) error
	// Estimate returns approximate number of unique keys added to the counter.
	Estimate() uint64
	// Reset flushes the counter.
	Reset()
}

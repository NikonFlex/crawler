package workerpool

import (
	"context"
	"sync"
)

// Accumulator is a function type used to aggregate values of type T into a result of type R.
// It must be thread-safe, as multiple goroutines will access the accumulator function concurrently.
// Each worker will produce intermediate results, which are combined with an initial or
// accumulated value.
type Accumulator[T, R any] func(current T, accum R) R

// Transformer is a function type used to transform an element of type T to another type R.
// The function is invoked concurrently by multiple workers, and therefore must be thread-safe
// to ensure data integrity when accessed across multiple goroutines.
// Each worker independently applies the transformer to its own subset of data, and although
// no shared state is expected, the transformer must handle any internal state in a thread-safe
// manner if present.
type Transformer[T, R any] func(current T) R

// Searcher is a function type for exploring data in a hierarchical manner.
// Each call to Searcher takes a parent element of type T and returns a slice of T representing
// its child elements. Since multiple goroutines may call Searcher concurrently, it must be
// thread-safe to ensure consistent results during recursive  exploration.
//
// Important considerations:
//  1. Searcher should be designed to avoid race conditions, particularly if it captures external
//     variables in closures.
//  2. The calling function must handle any state or values in closures, ensuring that
//     captured variables remain consistent throughout recursive or hierarchical search paths.
type Searcher[T any] func(parent T) []T

// Pool is the primary interface for managing worker pools, with support for three main
// operations: Transform, Accumulate, and List. Each operation takes an input channel, applies
// a transformation, accumulation, or list expansion, and returns the respective output.
type Pool[T, R any] interface {
	// Transform applies a transformer function to each item received from the input channel,
	// with results sent to the output channel. Transform operates concurrently, utilizing the
	// specified number of workers. The number of workers must be explicitly defined in the
	// configuration for this function to handle expected workloads effectively.
	// Since multiple workers may call the transformer function concurrently, it must be
	// thread-safe to prevent race conditions or unexpected results when handling shared or
	// internal state. Each worker independently applies the transformer function to its own
	// data subset.
	Transform(ctx context.Context, workers int, input <-chan T, transformer Transformer[T, R]) <-chan R

	// Accumulate applies an accumulator function to the items received from the input channel,
	// with results accumulated and sent to the output channel. The accumulator function must
	// be thread-safe, as multiple workers concurrently update the accumulated result.
	// The output channel will contain intermediate accumulated results as R
	Accumulate(ctx context.Context, workers int, input <-chan T, accumulator Accumulator[T, R]) <-chan R

	// List expands elements based on a searcher function, starting
	// from the given element. The searcher function finds child elements for each parent,
	// allowing exploration in a tree-like structure.
	// The number of workers should be configured based on the workload, ensuring each worker
	// independently processes assigned elements.
	List(ctx context.Context, workers int, start T, searcher Searcher[T])
}

type poolImpl[T, R any] struct{}

func New[T, R any]() *poolImpl[T, R] {
	return &poolImpl[T, R]{}
}

func (p *poolImpl[T, R]) Accumulate(
	ctx context.Context,
	workers int,
	input <-chan T,
	accumulator Accumulator[T, R],
) <-chan R {
	result := make(chan R)
	wg := new(sync.WaitGroup)

	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		// start worker
		go func() {
			// create var for accumulation
			var accumulation R
			defer func() {
				defer wg.Done() // show finish
				// Send the final accumulation when the worker completes.
				select {
				case <-ctx.Done(): // check done
					return
				case result <- accumulation: // push worker accumulation to result channel
				}
			}()

			// Process input items.
			for {
				select {
				case <-ctx.Done(): // check done
					return
				case value, ok := <-input: // take values from input channel
					if !ok {
						return
					}
					accumulation = accumulator(value, accumulation) // accumulate values
				}
			}
		}()
	}

	// Close the result channel once all workers finish.
	go func() {
		defer close(result)
		wg.Wait()
	}()

	return result
}

func (p *poolImpl[T, R]) processLayer(
	ctx context.Context,
	workers int,
	currentLayer []T,
	searcher Searcher[T],
) []T {
	var (
		nextLayer []T
		mu        sync.Mutex
		wg        sync.WaitGroup
	)

	parents := make(chan T)

	// Start worker goroutines to process parent nodes.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		// start worker
		go func() {
			defer wg.Done() // show finish

			for parent := range parents {
				select {
				case <-ctx.Done(): // check done
					return
				default:
					children := searcher(parent) // get children
					mu.Lock()
					nextLayer = append(nextLayer, children...)
					mu.Unlock()
				}
			}
		}()
	}

	// Send parent nodes to workers.
	for _, node := range currentLayer {
		select {
		case <-ctx.Done(): // check done
			close(parents)
			wg.Wait()
			return nil
		case parents <- node:
		}
	}
	close(parents)

	// Wait for all workers to finish.
	wg.Wait()

	return nextLayer
}

func (p *poolImpl[T, R]) List(
	ctx context.Context,
	workers int,
	start T,
	searcher Searcher[T],
) {
	currentLayer := []T{start}

	// Process each layer until no more nodes exist.
	for len(currentLayer) > 0 {
		currentLayer = p.processLayer(ctx, workers, currentLayer, searcher)
	}
}

func (p *poolImpl[T, R]) Transform(
	ctx context.Context,
	workers int,
	input <-chan T,
	transformer Transformer[T, R],
) <-chan R {
	result := make(chan R)
	wg := new(sync.WaitGroup)

	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		// start worker
		go func() {
			defer wg.Done() // show finish

			for {
				select {
				case <-ctx.Done(): // check done
					return
				case value, ok := <-input: // take value from input
					if !ok {
						return
					}

					select {
					case <-ctx.Done(): // check done
						return
					case result <- transformer(value): // transform value
					}
				}
			}
		}()
	}

	// Close the result channel once all workers finish.
	go func() {
		defer close(result)
		wg.Wait()
	}()

	return result
}

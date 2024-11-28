package crawler

import (
	"context"
	"crawler/internal/fs"
	"crawler/internal/workerpool"
	"encoding/json"
	"fmt"
	"sync"
)

// Configuration holds the configuration for the crawler, specifying the number of workers for
// file searching, processing, and accumulating tasks. The values for SearchWorkers, FileWorkers,
// and AccumulatorWorkers are critical to efficient performance and must be defined in
// every configuration.
type Configuration struct {
	SearchWorkers      int // Number of workers responsible for searching files.
	FileWorkers        int // Number of workers for processing individual files.
	AccumulatorWorkers int // Number of workers for accumulating results.
}

// Combiner is a function type that defines how to combine two values of type R into a single
// result. Combiner is not required to be thread-safe
//
// Combiner can either:
//   - Modify one of its input arguments to include the result of the other and return it,
//     or
//   - Create a new combined result based on the inputs and return it.
//
// It is assumed that type R has a neutral element (forming a monoid)
type Combiner[R any] func(current R, accum R) R

// Crawler represents a concurrent crawler implementing a map-reduce model with multiple workers
// to manage file processing, transformation, and accumulation tasks. The crawler is designed to
// handle large sets of files efficiently, assuming that all files can fit into memory
// simultaneously.
type Crawler[T, R any] interface {
	// Collect performs the full crawling operation, coordinating with the file system
	// and worker pool to process files and accumulate results. The result type R is assumed
	// to be a monoid, meaning there exists a neutral element for combination, and that
	// R supports an associative combiner operation.
	// The result of this collection process, after all reductions, is returned as type R.
	//
	// Important requirements:
	// 1. Number of workers in the Configuration is mandatory for managing workload efficiently.
	// 2. FileSystem and Accumulator must be thread-safe.
	// 3. Combiner does not need to be thread-safe.
	// 4. If an accumulator or combiner function modifies one of its arguments,
	//    it should return that modified value rather than creating a new one,
	//    or alternatively, it can create and return a new combined result.
	// 5. Context cancellation is respected across workers.
	// 6. Type T is derived by json-deserializing the file contents, and any issues in deserialization
	//    must be handled within the worker.
	// 7. The combiner function will wait for all workers to complete, ensuring no goroutine leaks
	//    occur during the process.
	Collect(
		ctx context.Context,
		fileSystem fs.FileSystem,
		root string,
		conf Configuration,
		accumulator workerpool.Accumulator[T, R],
		combiner Combiner[R],
	) (R, error)
}

type crawlerImpl[T, R any] struct{}

type executionContext[T, R any] struct {
	fileSystem fs.FileSystem
	conf       Configuration
	err        error
	once       sync.Once
	wg         sync.WaitGroup
}

func New[T, R any]() *crawlerImpl[T, R] {
	return &crawlerImpl[T, R]{}
}

func (c *crawlerImpl[T, R]) Collect(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	conf Configuration,
	accumulator workerpool.Accumulator[T, R],
	combiner Combiner[R],
) (R, error) {
	// Initialize the execution context to track errors and manage worker synchronization.
	execContext := executionContext[T, R]{fileSystem, conf, nil, sync.Once{}, sync.WaitGroup{}}

	filePaths := make(chan string) // Channel for discovered file paths.

	// Deserialize file data (JSON to type T) from the collected file paths.
	deserializedFiles := c.deserializeFiles(ctx, &execContext, filePaths)

	// Accumulate intermediate results from deserialized files using the accumulator.
	accumulations := c.accumulateFiles(ctx, &execContext, deserializedFiles, accumulator)

	wgCombining := new(sync.WaitGroup)
	var result R // Variable to hold the final combined result.

	// Combine all accumulated results into a single final result.
	c.combineAccumulations(ctx, wgCombining, accumulations, combiner, &result)

	// Begin the file path collection process from the root directory.
	c.collectFilePaths(ctx, &execContext, root, filePaths)

	// Wait for all workers and combining operations to finish before returning.
	execContext.wg.Wait()
	wgCombining.Wait()

	// Check for cancellation or errors before returning the result.
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	default:
		return result, execContext.err
	}
}

// combineAccumulations merges results produced by multiple workers into a single output
// using the provided Combiner function. Results are processed as they are received.
func (c *crawlerImpl[T, R]) combineAccumulations(ctx context.Context, wg *sync.WaitGroup, accumulations <-chan R, combiner Combiner[R], result *R) {
	wg.Add(1)
	go func() {
		defer wg.Done() // Ensure the WaitGroup counter is decremented.
		for {
			select {
			case <-ctx.Done(): // Stop if the context is canceled.
				return
			case accum, ok := <-accumulations: // Receive intermediate results.
				if !ok {
					return
				}
				*result = combiner(accum, *result) // Combine the current result with the accumulated data.
			}
		}
	}()
}

// accumulateFiles uses the accumulator function to process deserialized file data and
// produce intermediate results. The results are returned via a channel.
func (c *crawlerImpl[T, R]) accumulateFiles(ctx context.Context, execContext *executionContext[T, R], files <-chan T, accumulator workerpool.Accumulator[T, R]) <-chan R {
	return workerpool.New[T, R]().Accumulate(ctx, execContext.conf.AccumulatorWorkers, files, accumulator)
}

// deserializeFile reads a file and decodes its JSON content into an instance of type T.
// Errors during file operations or decoding are captured and reported.
func (c *crawlerImpl[T, R]) deserializeFile(execContext *executionContext[T, R], filePath string) T {
	var zero T // Default zero value for type T.
	// try to open file
	file, fileOpenErr := execContext.fileSystem.Open(filePath)
	if fileOpenErr != nil {
		c.throwError(fileOpenErr, execContext)
		return zero
	}

	// try to close file
	defer func(file fs.File) {
		if fileCloseErr := file.Close(); fileCloseErr != nil {
			fmt.Println(fileCloseErr)
		}
	}(file)

	// try to read file and convert to json
	var t T
	decoder := json.NewDecoder(file)
	if fileDecodeErr := decoder.Decode(&t); fileDecodeErr != nil {
		c.throwError(fileDecodeErr, execContext)
		return zero
	}

	return t
}

// deserializeFiles processes file paths to generate deserialized data (type T).
// Any errors during file reading or decoding are managed by the error handling mechanism.
func (c *crawlerImpl[T, R]) deserializeFiles(ctx context.Context, execContext *executionContext[T, R], filePaths chan string) <-chan T {
	return workerpool.New[string, T]().Transform(ctx, execContext.conf.FileWorkers, filePaths, func(filePath string) T {
		defer func() {
			// recovering from panics
			if r := recover(); r != nil {
				var err error
				if e, ok := r.(error); ok {
					err = e
				} else {
					err = fmt.Errorf("%v", r)
				}
				c.throwError(err, execContext)
			}
		}()
		return c.deserializeFile(execContext, filePath)
	})
}

// exploreDir reads a directory to collect child directories and file paths.
// File paths are sent to the output channel, while child directories are returned for further exploration.
func (c *crawlerImpl[T, R]) exploreDir(
	ctx context.Context,
	parent string,
	execContext *executionContext[T, R],
	filePaths chan<- string,
) []string {
	// try to read dir
	entries, err := execContext.fileSystem.ReadDir(parent)
	if err != nil {
		c.throwError(err, execContext)
		return nil
	}

	var children []string
	for _, entry := range entries {
		fullPath := execContext.fileSystem.Join(parent, entry.Name())
		if entry.IsDir() {
			children = append(children, fullPath)
		} else {
			select {
			case <-ctx.Done(): // Stop if the context is canceled.
				return nil
			case filePaths <- fullPath: // Send the file path to the output channel.
			}
		}
	}
	return children
}

// collectFilePaths recursively explores directories starting from the root,
// sending discovered file paths to the provided channel for processing.
func (c *crawlerImpl[T, R]) collectFilePaths(ctx context.Context, execContext *executionContext[T, R], root string, files chan string) {
	workerpool.New[string, T]().List(ctx, execContext.conf.SearchWorkers, root, func(parent string) []string {
		defer func() {
			// recovering from panics
			if r := recover(); r != nil {
				var err error
				if e, ok := r.(error); ok {
					err = e
				} else {
					err = fmt.Errorf("%v", r)
				}
				c.throwError(err, execContext)
			}
		}()
		return c.exploreDir(ctx, parent, execContext, files)
	})
	close(files) // Close the channel once all paths are collected.
}

// onErrorHappened sets the first encountered error and logs it.
func (c *crawlerImpl[T, R]) onErrorHappened(err error, execContext *executionContext[T, R]) {
	execContext.err = err
	fmt.Println("ERROR:", err)
}

// throwError ensures that error handling logic is executed only once,
// preventing duplicate or conflicting error states.
func (c *crawlerImpl[T, R]) throwError(err error, execContext *executionContext[T, R]) {
	execContext.once.Do(func() { c.onErrorHappened(err, execContext) })
}

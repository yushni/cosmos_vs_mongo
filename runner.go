package main

import (
	"github.com/cheggaaa/pb"
	"golang.org/x/sync/errgroup"
)

type runner struct {
	total, parallelRequests, priceOfIteration int
}

func (r *runner) run(f func() error) error {
	bar := pb.StartNew(r.total)
	guard := make(chan struct{}, r.parallelRequests)
	errGroup := errgroup.Group{}

	for i := 0; i < r.total; i += r.priceOfIteration {
		guard <- struct{}{}

		errGroup.Go(func() error {
			defer func() { <-guard }()

			if err := f(); err != nil {
				return err
			}

			bar.Add(r.priceOfIteration)
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return err
	}

	bar.Finish()
	return nil
}

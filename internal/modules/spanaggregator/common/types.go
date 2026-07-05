package common

var HistogramBuckets = []float64{
	2, 4, 6, 8, 10, 50, 100, 200, 400, 800, 1000, 1400, 2000, 5000, 10000, 15000,
}

type AggState struct {
	Count       uint64
	Sum         float64
	BucketCount []uint64
}

func NewAggState() *AggState {
	return &AggState{
		BucketCount: make([]uint64, len(HistogramBuckets)+1),
	}
}

func (s *AggState) Add(durMs float64) {
	s.Count++
	s.Sum += durMs
	bucketIdx := len(HistogramBuckets)
	for i, bound := range HistogramBuckets {
		if durMs <= bound {
			bucketIdx = i
			break
		}
	}
	s.BucketCount[bucketIdx]++
}

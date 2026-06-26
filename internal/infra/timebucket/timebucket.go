// Package timebucket provides the 5-minute bucket alignment writers use when
// keying timestamps for ClickHouse rows.
package timebucket

const BucketSeconds int64 = 300

func BucketStart(unixSeconds int64) uint32 {
	return uint32((unixSeconds / BucketSeconds) * BucketSeconds)
}

// Package timebucket provides the 5-minute bucket alignment writers use when
// keying timestamps for ClickHouse rows.
package timebucket

const BucketSeconds int64 = 300

// BucketStart truncates a Unix-second timestamp to its 5-minute bucket.
// Returned as UInt32 for lossless round-trips through ClickHouse.
func BucketStart(unixSeconds int64) uint32 {
	return uint32((unixSeconds / BucketSeconds) * BucketSeconds)
}

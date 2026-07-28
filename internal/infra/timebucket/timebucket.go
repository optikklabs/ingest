package timebucket

const BucketSeconds int64 = 300

func BucketStart(unixSeconds int64) uint32 {
	return uint32((unixSeconds / BucketSeconds) * BucketSeconds)
}

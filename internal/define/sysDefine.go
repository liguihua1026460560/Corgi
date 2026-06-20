package define

const (
	PebbleObjMetaDeleteMarker = "~"
	PebblePartPrefix          = "!"
	PebblePartMetaPrefix      = "@"
	PebbleFileMetaPrefix      = "#"
	PebbleBucketMetaPrefix    = "$"
	PebbleStaticPrefix        = "&"
	PebbleVersionPrefix       = "*"
	PebbleLifeCyclePrefix     = "+"
	PebbleLastAccessPrefix    = "_"
	PebbleFileSystemPrefix    = "."
	PebbleSpecialKey          = "%"
	PebbleLatestKey           = "-"
	PebbleSmallestKey         = "0"
	PebbleUnsynchronizedKey   = "|"
	PebbleSingleSyncKey       = ","
	PebbleDeduplicateKey      = "^"
	PebbleComponentVideoKey   = ":"
	PebbleComponentImageKey   = "!"
	PebbleInodePrefix         = "("
	PebbleChunkFileKey        = ")"
	PebbleESKey               = "'"
	PebbleCookieKey           = "<"
	PebbleSTSTokenKey         = "="

	PebbleAggregationMetaPrefix    = "?"
	PebbleAggregationRatePrefix    = "%"
	PebbleAggregationUndoLogPrefix = ">"

	PebbleCacheOrderedKey = "{"
	PebbleCacheAccessKey  = "}"

	PebbleFileSystemFileNum  = ".2.num"
	PebbleFileSystemSize     = ".2.size"
	PebbleFileSystemUsedSize = ".2.used"
)

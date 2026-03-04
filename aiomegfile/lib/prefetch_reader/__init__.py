from .http_prefetch_reader import AioHttpPrefetchReader
from .s3_prefetch_reader import AioS3PrefetchReader

__all__ = [
    "AioHttpPrefetchReader",
    "AioS3PrefetchReader",
]

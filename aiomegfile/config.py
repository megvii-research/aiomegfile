import os


S3_CLIENT_CACHE_MODE = os.getenv(
    "AIOMEGFILE_S3_CLIENT_CACHE_MODE") or "thread_local"

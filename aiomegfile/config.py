import os

GLOBAL_MAX_WORKERS = int(os.getenv("MEGFILE_MAX_WORKERS") or 8)

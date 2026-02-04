import os

from aiomegfile.utils.parse import parse_boolean, parse_quantity

DEFAULT_MAX_RETRY_TIMES = int(os.getenv("AIOMEGFILE_MAX_RETRY_TIMES") or 10)
GLOBAL_MAX_WORKERS = int(os.getenv("AIOMEGFILE_MAX_WORKERS") or 8)

DEFAULT_WRITER_BLOCK_AUTOSCALE = not os.getenv("AIOMEGFILE_WRITER_BLOCK_SIZE")
if os.getenv("AIOMEGFILE_WRITER_BLOCK_AUTOSCALE"):
    DEFAULT_WRITER_BLOCK_AUTOSCALE = parse_boolean(
        os.environ["AIOMEGFILE_WRITER_BLOCK_AUTOSCALE"]
    )
# Multi-upload in aws s3 has a maximum of 10,000 parts,
# so the maximum supported file size is AIOMEGFILE_WRITE_BLOCK_SIZE * 10,000,
# the largest object that can be uploaded in a single PUT is 5 TB in aws s3.
WRITER_BLOCK_SIZE = parse_quantity(
    os.getenv("AIOMEGFILE_WRITER_BLOCK_SIZE") or 8 * 2**20
)
if WRITER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'AIOMEGFILE_WRITER_BLOCK_SIZE' must bigger than 0, got {WRITER_BLOCK_SIZE}"
    )
WRITER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("AIOMEGFILE_WRITER_MAX_BUFFER_SIZE") or 128 * 2**20
)

# Reader configuration
READER_BLOCK_SIZE = parse_quantity(
    os.getenv("AIOMEGFILE_READER_BLOCK_SIZE") or 8 * 2**20
)
if READER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'AIOMEGFILE_READER_BLOCK_SIZE' must bigger than 0, got {READER_BLOCK_SIZE}"
    )
READER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("AIOMEGFILE_READER_MAX_BUFFER_SIZE") or 128 * 2**20
)
READER_LAZY_PREFETCH = parse_boolean(
    os.getenv("AIOMEGFILE_READER_LAZY_PREFETCH") or "false"
)

# S3 configuration
S3_MAX_RETRY_TIMES = int(
    os.getenv("AIOMEGFILE_S3_MAX_RETRY_TIMES") or DEFAULT_MAX_RETRY_TIMES
)

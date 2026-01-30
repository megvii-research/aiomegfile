import os

from aiomegfile.utils.parse import parse_boolean, parse_quantity

DEFAULT_MAX_RETRY_TIMES = int(os.getenv("AIOMEGFILE_MAX_RETRY_TIMES") or 10)
GLOBAL_MAX_WORKERS = int(os.getenv("AIOMEGFILE_MAX_WORKERS") or 8)

DEFAULT_WRITER_BLOCK_AUTOSCALE = not os.getenv("MEGFILE_WRITER_BLOCK_SIZE")
if os.getenv("MEGFILE_WRITER_BLOCK_AUTOSCALE"):
    DEFAULT_WRITER_BLOCK_AUTOSCALE = parse_boolean(
        os.environ["MEGFILE_WRITER_BLOCK_AUTOSCALE"]
    )
# Multi-upload in aws s3 has a maximum of 10,000 parts,
# so the maximum supported file size is MEGFILE_WRITE_BLOCK_SIZE * 10,000,
# the largest object that can be uploaded in a single PUT is 5 TB in aws s3.
WRITER_BLOCK_SIZE = parse_quantity(os.getenv("MEGFILE_WRITER_BLOCK_SIZE") or 8 * 2**20)
if WRITER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_WRITER_BLOCK_SIZE' must bigger than 0, got {WRITER_BLOCK_SIZE}"
    )
WRITER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("MEGFILE_WRITER_MAX_BUFFER_SIZE") or 128 * 2**20
)

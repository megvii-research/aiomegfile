# Async Optimization TODOs

- [x] aiomegfile/filesystem/s3.py `exists`: run `is_file` and `is_dir` concurrently with `asyncio.gather` or `TaskGroup` to reduce round trips.
- [x] aiomegfile/filesystem/s3.py `glob_stat`: parallelize `_glob_stat_single_path` across grouped prefixes/buckets with a bounded semaphore and gather results.
- [x] aiomegfile/filesystem/s3.py `remove`: submit `delete_objects` per page concurrently (bounded) after listing pages.
- [x] aiomegfile/filesystem/s3.py `move`: copy each object with concurrent `copy_object` tasks (bounded), then remove source.
- [ ] aiomegfile/filesystem/s3.py `scanfile`: batch `is_symlink`/`head_object` checks for zero-size objects concurrently.
- [ ] aiomegfile/filesystem/s3.py `_group_src_paths_by_block`: fetch `stat` sizes concurrently before grouping.
- [ ] aiomegfile/filesystem/s3.py `MultiPartWriter.upload_part_by_paths`: fetch multiple source objects/ranges concurrently and assemble in order.
- [ ] aiomegfile/smart_path.py `copy` (directory branch): copy files concurrently with a bounded semaphore.
- [ ] aiomegfile/smart.py `smart_sync`: schedule `smart_copy_file` operations concurrently (bounded), keep callbacks safe.
- [ ] aiomegfile/smart.py `_iter_file_stats`: for `followlinks=True`, resolve symlinks and `stat` concurrently per directory.
- [ ] aiomegfile/smart_path.py `walk`: for symlink entries, resolve `is_dir(followlinks=True)` concurrently per directory level.
- [ ] aiomegfile/cli.py `_glob_stat` fallback: batch `filesystem.stat` calls concurrently.
- [ ] aiomegfile/cli.py `_ls` (long list with symlinks): resolve `smart_readlink` concurrently.
- [ ] aiomegfile/filesystem/local.py `open`/`move`/`scandir`/`scanfile`: offload blocking OS calls via `asyncio.to_thread` or batch them to avoid event loop blocking.

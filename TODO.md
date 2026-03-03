# aiomegfile vs megfile 接口差异（smart.py & SmartPath）

> 说明：对比 `aiomegfile/smart.py`、`aiomegfile/smart_path.py` 与 megfile 仓库同名文件的公开接口。

megfile 仓库地址：/home/vscode/project/megfile

## smart.py 函数差异

### megfile 有而 aiomegfile 缺失
- [ ] `register_copy_func`
- [x] `smart_cache`
- [x] `smart_combine_open`
- [x] `smart_getmd5`
- [x] `smart_glob_stat`
- [x] `smart_lstat`
- [x] `smart_scan_stat`
- [ ] `smart_sync_with_progress`

### 同名函数的参数差异
- [ ] `smart_copy`
  - aiomegfile: `src_path, dst_path, followlinks`
  - megfile: `src_path, dst_path, callback, followlinks, overwrite`
  - aiomegfile 缺少参数: `callback, overwrite`
- [ ] `smart_glob`
  - aiomegfile: `path, recursive`
  - megfile: `pathname, recursive, missing_ok`
  - aiomegfile 缺少参数: `missing_ok`
- [ ] `smart_iglob`
  - aiomegfile: `path, recursive`
  - megfile: `pathname, recursive, missing_ok`
  - aiomegfile 缺少参数: `missing_ok`
- [ ] `smart_move`
  - aiomegfile: `src_path, dst_path`
  - megfile: `src_path, dst_path, overwrite`
  - aiomegfile 缺少参数: `overwrite`
- [ ] `smart_open`
  - aiomegfile: `path, mode, buffering, encoding, errors, newline`
  - megfile: `path, mode, encoding, errors, s3_open_func, **options`
  - aiomegfile 缺少参数: `**options, s3_open_func`
- [ ] `smart_rename`
  - aiomegfile: `src_path, dst_path`
  - megfile: `src_path, dst_path, overwrite`
  - aiomegfile 缺少参数: `overwrite`
- [ ] `smart_sync`
  - aiomegfile: `src_path, dst_path, callback, callback_after_copy_file, followlinks, force, overwrite`
  - megfile: `src_path, dst_path, callback, followlinks, callback_after_copy_file, src_file_stats, map_func, force, overwrite`
  - aiomegfile 缺少参数: `map_func, src_file_stats`
- [ ] `smart_touch`
  - aiomegfile: `path, exist_ok`
  - megfile: `path`
  - megfile 缺少参数: `exist_ok`

## SmartPath 接口差异（smart_path.py）

### 方法（methods）
#### megfile 有而 aiomegfile 缺失
- [ ] `abspath`
- [ ] `chmod`
- [ ] `cwd`
- [ ] `expanduser`
- [x] `getmtime`
- [x] `getsize`
- [x] `glob_stat`
- [ ] `group`
- [ ] `home`
- [ ] `is_absolute`
- [ ] `lchmod`
- [ ] `listdir`
- [ ] `load`
- [x] `md5`
- [ ] `owner`
- [ ] `realpath`
- [ ] `relpath`
- [ ] `save`
- [x] `scan`
- [x] `scan_stat`
- [ ] `scandir`
- [ ] `symlink`
- [ ] `utime`

### 属性（properties）

### SmartPath 同名方法的参数差异

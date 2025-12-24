"""Tests for glob module."""

import os
import typing as T
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from aiomegfile.lib.glob import (
    FSFunc,
    GlobFileEntry,
    escape,
    escape_brace,
    get_non_glob_dir,
    glob,
    globlize,
    has_magic,
    has_magic_ignore_brace,
    iglob,
    unescape,
    ungloblize,
)


class RealFileSystemAdapter:
    """Adapter to make real filesystem compatible with FSFunc interface."""

    def __init__(self, base_path: str = ""):
        """
        Initialize adapter with optional base path.

        Args:
            base_path: Base directory path for relative paths.
        """
        self.base_path = base_path

    def _resolve(self, path: str) -> str:
        """Resolve path relative to base_path if needed."""
        if self.base_path and not os.path.isabs(path):
            return os.path.join(self.base_path, path)
        return path

    async def exists(self, path: str) -> bool:
        """Check if path exists."""
        return os.path.exists(self._resolve(path))

    async def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        return os.path.isdir(self._resolve(path))

    @asynccontextmanager
    async def scandir(self, dirname: str):
        """Scan directory and yield entries."""

        async def _iter():
            resolved = self._resolve(dirname) if dirname else self._resolve(".")
            if not os.path.exists(resolved):
                return

            for entry in os.scandir(resolved):
                yield GlobFileEntry(name=entry.name, is_dir=entry.is_dir())

        yield _iter()


def create_fs_func(base_path: str = "") -> FSFunc:
    """Create FSFunc from real filesystem with optional base path."""
    adapter = RealFileSystemAdapter(base_path)
    return FSFunc(
        exists=adapter.exists,
        isdir=adapter.isdir,
        scandir=adapter.scandir,
    )


def setup_files(base_path: Path, files: T.Dict[str, bool]) -> None:
    """
    Setup files and directories in the given base path.

    Args:
        base_path: Base directory path.
        files: Dict mapping relative paths to is_dir boolean.
               True means directory, False means file.
    """
    # Sort to ensure parent directories are created before children
    for path in sorted(files.keys()):
        is_dir = files[path]
        full_path = base_path / path
        if is_dir:
            full_path.mkdir(parents=True, exist_ok=True)
        else:
            # Ensure parent directory exists
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.touch()


class TestHasMagic:
    """Tests for has_magic function."""

    def test_no_magic(self):
        assert not has_magic("foo/bar/baz.txt")
        assert not has_magic("/path/to/file")
        assert not has_magic("simple")

    def test_asterisk(self):
        assert has_magic("*.txt")
        assert has_magic("foo/*.py")
        assert has_magic("*")

    def test_question_mark(self):
        assert has_magic("file?.txt")
        assert has_magic("?")

    def test_bracket(self):
        assert has_magic("[abc].txt")
        assert has_magic("file[0-9].txt")

    def test_brace(self):
        assert has_magic("{a,b}.txt")
        assert has_magic("file.{py,txt}")


class TestHasMagicIgnoreBrace:
    """Tests for has_magic_ignore_brace function."""

    def test_no_magic_with_brace(self):
        assert not has_magic_ignore_brace("{a,b}.txt")
        assert not has_magic_ignore_brace("file.{py,txt}")

    def test_has_magic_with_brace(self):
        assert has_magic_ignore_brace("{a,b}/*.txt")
        assert has_magic_ignore_brace("file?.{py,txt}")

    def test_has_magic_without_brace(self):
        assert has_magic_ignore_brace("*.txt")
        assert has_magic_ignore_brace("file?.txt")


class TestEscape:
    """Tests for escape function."""

    def test_escape_asterisk(self):
        assert escape("file*.txt") == "file[*].txt"

    def test_escape_question_mark(self):
        assert escape("file?.txt") == "file[?].txt"

    def test_escape_bracket(self):
        assert escape("file[0].txt") == "file[[]0].txt"

    def test_escape_brace(self):
        assert escape("file{a,b}.txt") == "file[{]a,b}.txt"

    def test_escape_multiple(self):
        assert escape("*?[{") == "[*][?][[][{]"

    def test_no_escape_needed(self):
        assert escape("normal.txt") == "normal.txt"


class TestUnescape:
    """Tests for unescape function."""

    def test_unescape_asterisk(self):
        assert unescape("file[*].txt") == "file*.txt"

    def test_unescape_question_mark(self):
        assert unescape("file[?].txt") == "file?.txt"

    def test_unescape_bracket(self):
        assert unescape("file[[].txt") == "file[.txt"

    def test_unescape_multiple(self):
        assert unescape("[*][?][[]") == "*?["

    def test_no_unescape_needed(self):
        assert unescape("normal.txt") == "normal.txt"


class TestEscapeBrace:
    """Tests for escape_brace function."""

    def test_escape_brace_only(self):
        assert escape_brace("{a,b}") == "[{]a,b}"

    def test_no_escape_other_magic(self):
        # Only braces should be escaped
        assert escape_brace("*.txt") == "*.txt"
        assert escape_brace("file?.txt") == "file?.txt"

    def test_escape_brace_in_path(self):
        assert escape_brace("/path/{a,b}/file.txt") == "/path/[{]a,b}/file.txt"


class TestGloblize:
    """Tests for globlize function."""

    def test_single_path(self):
        # Same path returns itself
        result = globlize(["foo/bar.txt"])
        assert result == "foo/bar.txt"

    def test_common_prefix_suffix(self):
        paths = ["/path/a/file.txt", "/path/b/file.txt"]
        result = globlize(paths)
        assert result == "/path/{a,b}/file.txt"

    def test_common_prefix_only(self):
        paths = ["/path/file1.txt", "/path/file2.txt"]
        result = globlize(paths)
        assert "{" in result
        assert "1,2" in result or "2,1" in result

    def test_common_suffix_with_dot(self):
        paths = ["foo.a.txt", "foo.b.txt"]
        result = globlize(paths)
        assert "{" in result

    def test_no_common_parts(self):
        paths = ["a.txt", "b.txt"]
        result = globlize(paths)
        assert result == "{a,b}.txt"


class TestUngloblize:
    """Tests for ungloblize function."""

    def test_no_brace(self):
        result = ungloblize("foo/bar.txt")
        assert result == ["foo/bar.txt"]

    def test_simple_brace(self):
        result = ungloblize("{a,b}.txt")
        assert sorted(result) == sorted(["a.txt", "b.txt"])

    def test_multiple_options(self):
        result = ungloblize("{a,b,c}.txt")
        assert sorted(result) == sorted(["a.txt", "b.txt", "c.txt"])

    def test_brace_in_path(self):
        result = ungloblize("/path/{a,b}/file.txt")
        assert sorted(result) == sorted(["/path/a/file.txt", "/path/b/file.txt"])

    def test_escaped_brace(self):
        # Test with no braces to expand
        result = ungloblize("normal.txt")
        assert result == ["normal.txt"]

    def test_nested_expansion(self):
        # Multiple braces
        result = ungloblize("{a,b}.{txt,py}")
        assert len(result) == 4
        assert "a.txt" in result
        assert "a.py" in result
        assert "b.txt" in result
        assert "b.py" in result


class TestGetNonGlobDir:
    """Tests for get_non_glob_dir function."""

    def test_no_glob(self):
        assert get_non_glob_dir("/path/to/file.txt") == "/path/to/file.txt"

    def test_glob_at_end(self):
        assert get_non_glob_dir("/path/to/*.txt") == "/path/to"

    def test_glob_in_middle(self):
        assert get_non_glob_dir("/path/*/file.txt") == "/path"

    def test_glob_at_start(self):
        assert get_non_glob_dir("*.txt") == "."

    def test_with_protocol(self):
        assert get_non_glob_dir("s3://bucket/path/*.txt") == "s3://bucket/path"

    def test_with_protocol_glob_early(self):
        assert get_non_glob_dir("s3://bucket/*/*.txt") == "s3://bucket"

    def test_absolute_path_with_glob(self):
        assert get_non_glob_dir("/foo/bar/*.py") == "/foo/bar"

    def test_relative_path_with_glob(self):
        assert get_non_glob_dir("foo/bar/*.py") == "foo/bar"

    def test_only_glob(self):
        assert get_non_glob_dir("*") == "."

    def test_brace_is_magic(self):
        assert get_non_glob_dir("/path/{a,b}/file.txt") == "/path"


class TestGlob:
    """Tests for glob function with tmp_path."""

    @pytest.fixture
    def simple_fs(self, tmp_path):
        """Simple filesystem with a few files."""
        files = {
            "dir": True,
            "dir/file1.txt": False,
            "dir/file2.txt": False,
            "dir/file3.py": False,
            "dir/sub": True,
            "dir/sub/nested.txt": False,
        }
        setup_files(tmp_path, files)
        return create_fs_func(str(tmp_path))

    @pytest.fixture
    def complex_fs(self, tmp_path):
        """Complex filesystem for advanced glob tests."""
        files = {
            "root": True,
            "root/a.txt": False,
            "root/b.txt": False,
            "root/c.py": False,
            "root/sub1": True,
            "root/sub1/x.txt": False,
            "root/sub1/y.py": False,
            "root/sub2": True,
            "root/sub2/z.txt": False,
            "root/sub1/deep": True,
            "root/sub1/deep/file.txt": False,
            ".hidden": True,
            ".hidden/secret.txt": False,
        }
        setup_files(tmp_path, files)
        return create_fs_func(str(tmp_path))

    async def test_glob_asterisk(self, simple_fs):
        """Test * wildcard matching."""
        result = await glob("dir/*.txt", simple_fs)
        assert len(result) == 2
        assert "dir/file1.txt" in result
        assert "dir/file2.txt" in result

    async def test_glob_question_mark(self, simple_fs):
        """Test ? wildcard matching."""
        result = await glob("dir/file?.txt", simple_fs)
        assert len(result) == 2

    async def test_glob_no_match(self, simple_fs):
        """Test pattern that matches nothing."""
        result = await glob("dir/*.md", simple_fs)
        assert result == []

    async def test_glob_exact_match(self, simple_fs):
        """Test exact path (no wildcards)."""
        result = await glob("dir/file1.txt", simple_fs)
        assert result == ["dir/file1.txt"]

    async def test_glob_nonexistent(self, simple_fs):
        """Test pattern for nonexistent file."""
        result = await glob("dir/nonexistent.txt", simple_fs)
        assert result == []

    async def test_glob_recursive(self, simple_fs):
        """Test ** recursive matching."""
        result = await glob("dir/**", simple_fs, recursive=True)
        assert "dir/sub/nested.txt" in result

    async def test_glob_hidden_files_not_matched(self, complex_fs):
        """Test that hidden files are not matched by *."""
        result = await glob("*", complex_fs)
        # .hidden should not be in result
        assert ".hidden" not in result
        assert "root" in result

    async def test_glob_directory_only(self, simple_fs):
        """Test pattern ending with /."""
        result = await glob("dir/", simple_fs)
        assert result == ["dir/"]


class TestIglob:
    """Tests for iglob async iterator."""

    @pytest.fixture
    def simple_fs(self, tmp_path):
        """Simple filesystem."""
        files = {
            "a.txt": False,
            "b.txt": False,
            "c.py": False,
        }
        setup_files(tmp_path, files)
        return create_fs_func(str(tmp_path))

    async def test_iglob_yields_items(self, simple_fs):
        """Test that iglob yields items one by one."""
        items = []
        async for item in iglob("*.txt", simple_fs):
            items.append(item)
        assert len(items) == 2

    async def test_iglob_recursive(self, tmp_path):
        """Test recursive iglob with **."""
        files = {
            "dir": True,
            "dir/sub": True,
            "dir/sub/file.txt": False,
        }
        setup_files(tmp_path, files)
        fs_func = create_fs_func(str(tmp_path))
        items = []
        async for item in iglob("dir/**", fs_func, recursive=True):
            items.append(item)
        assert any("file.txt" in item for item in items)


class TestGlobWithProtocol:
    """Tests for glob with protocol prefixes.

    Note: Protocol paths like 's3://' or 'file://' are not real filesystem paths,
    so we use a simple mock adapter for these tests.
    """

    async def test_glob_with_file_protocol(self):
        """Test glob with file:// protocol."""
        # For protocol-based paths, we need a custom mock since real filesystem
        # doesn't support protocol prefixes
        files = {
            "file://bucket": True,
            "file://bucket/a.txt": False,
            "file://bucket/b.txt": False,
        }
        mock_fs = MockFSForProtocol(files)
        fs_func = FSFunc(
            exists=mock_fs.exists,
            isdir=mock_fs.isdir,
            scandir=mock_fs.scandir,
        )
        result = await glob("file://bucket/*.txt", fs_func)
        assert len(result) == 2

    async def test_glob_with_s3_like_protocol(self):
        """Test glob with s3:// protocol."""
        files = {
            "s3://mybucket": True,
            "s3://mybucket/data": True,
            "s3://mybucket/data/file1.csv": False,
            "s3://mybucket/data/file2.csv": False,
        }
        mock_fs = MockFSForProtocol(files)
        fs_func = FSFunc(
            exists=mock_fs.exists,
            isdir=mock_fs.isdir,
            scandir=mock_fs.scandir,
        )
        result = await glob("s3://mybucket/data/*.csv", fs_func)
        assert len(result) == 2


class MockFSForProtocol:
    """Mock filesystem for protocol-based paths (s3://, file://, etc.)."""

    def __init__(self, files: T.Dict[str, bool]):
        self.files = files

    async def exists(self, path: str) -> bool:
        path = path.rstrip("/")
        return path in self.files or any(f.startswith(path + "/") for f in self.files)

    async def isdir(self, path: str) -> bool:
        path = path.rstrip("/")
        return self.files.get(path, False)

    @asynccontextmanager
    async def scandir(self, dirname: str):
        async def _iter():
            dirname_normalized = dirname.rstrip("/")
            for path, is_dir in self.files.items():
                if dirname_normalized:
                    if not path.startswith(dirname_normalized + "/"):
                        continue
                    rel_path = path[len(dirname_normalized) + 1 :]
                else:
                    rel_path = path
                if "/" not in rel_path and rel_path:
                    yield GlobFileEntry(name=rel_path, is_dir=is_dir)

        yield _iter()


class TestGlobEdgeCases:
    """Tests for edge cases in glob."""

    async def test_glob_empty_directory(self, tmp_path):
        """Test glob on empty directory."""
        (tmp_path / "empty").mkdir()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("empty/*", fs_func)
        assert result == []

    async def test_glob_with_multiple_extensions(self, tmp_path):
        """Test glob matching files with multiple extensions."""
        (tmp_path / "file.tar.gz").touch()
        (tmp_path / "file.tar.bz2").touch()
        (tmp_path / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("*.tar.*", fs_func)
        assert len(result) == 2

    async def test_glob_bracket_pattern(self, tmp_path):
        """Test glob with bracket character class."""
        (tmp_path / "file1.txt").touch()
        (tmp_path / "file2.txt").touch()
        (tmp_path / "file3.txt").touch()
        (tmp_path / "filea.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("file[12].txt", fs_func)
        assert len(result) == 2
        assert "file1.txt" in result
        assert "file2.txt" in result

    async def test_glob_with_directory_in_pattern(self, tmp_path):
        """Test glob when directory part also has magic."""
        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir2").mkdir()
        (tmp_path / "dir1" / "file.txt").touch()
        (tmp_path / "dir2" / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("dir*/file.txt", fs_func)
        assert len(result) == 2

    async def test_glob_directory_not_exists(self, tmp_path):
        """Test glob when parent directory doesn't exist."""
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("nonexistent/*.txt", fs_func)
        assert result == []


class TestRealFilesystem:
    """Tests for glob with real filesystem."""

    @pytest.fixture
    def temp_dir(self, tmp_path):
        """Create a temporary directory structure."""
        # Create directory structure
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "deep").mkdir()
        (tmp_path / ".hidden").mkdir()

        # Create files
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")
        (tmp_path / "file3.py").write_text("content3")
        (tmp_path / "subdir" / "nested.txt").write_text("nested")
        (tmp_path / "subdir" / "deep" / "deep.txt").write_text("deep")
        (tmp_path / ".hidden" / "secret.txt").write_text("secret")

        return tmp_path

    async def test_real_glob_asterisk(self, temp_dir):
        """Test real filesystem glob with *."""
        from aiomegfile.smart_path import SmartPath

        p = SmartPath(str(temp_dir))
        result = await p.glob("*.txt")
        assert len(result) == 2
        names = [os.path.basename(str(r)) for r in result]
        assert "file1.txt" in names
        assert "file2.txt" in names

    async def test_real_glob_recursive(self, temp_dir):
        """Test real filesystem glob with **."""
        from aiomegfile.smart_path import SmartPath

        p = SmartPath(str(temp_dir))
        result = await p.rglob("*.txt")
        # Should find file1.txt, file2.txt, nested.txt, deep.txt
        assert len(result) >= 4

    async def test_real_glob_question_mark(self, temp_dir):
        """Test real filesystem glob with ?."""
        from aiomegfile.smart_path import SmartPath

        p = SmartPath(str(temp_dir))
        result = await p.glob("file?.txt")
        assert len(result) == 2


class TestBraceExpansion:
    """Tests for {a,b} brace expansion syntax."""

    def test_ungloblize_simple_brace_expansion(self):
        """Test simple {a,b} expansion."""
        result = ungloblize("file.{txt,py}")
        assert sorted(result) == sorted(["file.txt", "file.py"])

    def test_ungloblize_multiple_braces(self):
        """Test multiple {a,b} patterns in path."""
        result = ungloblize("{src,lib}/{a,b}.py")
        assert len(result) == 4
        assert "src/a.py" in result
        assert "src/b.py" in result
        assert "lib/a.py" in result
        assert "lib/b.py" in result

    def test_ungloblize_brace_with_numbers(self):
        """Test {1,2,3} number expansion."""
        result = ungloblize("file{1,2,3}.txt")
        assert sorted(result) == sorted(["file1.txt", "file2.txt", "file3.txt"])

    def test_ungloblize_brace_with_extensions(self):
        """Test common extension pattern."""
        result = ungloblize("main.{js,ts,jsx,tsx}")
        assert len(result) == 4
        assert "main.js" in result
        assert "main.ts" in result

    def test_ungloblize_nested_dirs_with_brace(self):
        """Test brace in nested directory."""
        result = ungloblize("/root/{config,settings}/app.json")
        assert sorted(result) == sorted(
            ["/root/config/app.json", "/root/settings/app.json"]
        )

    def test_globlize_creates_brace_pattern(self):
        """Test that globlize creates {a,b} patterns."""
        paths = ["file.txt", "file.py", "file.md"]
        result = globlize(paths)
        assert "{" in result
        assert "}" in result

    def test_globlize_and_ungloblize_roundtrip(self):
        """Test globlize and ungloblize are inverse operations."""
        original = ["src/a.txt", "src/b.txt", "src/c.txt"]
        globbed = globlize(original)
        unglobbed = ungloblize(globbed)
        assert sorted(unglobbed) == sorted(original)


class TestGlobRecursivePatterns:
    """Tests for ** recursive patterns."""

    async def test_glob_double_star_at_start(self, tmp_path):
        """Test ** at the start of pattern."""
        (tmp_path / "a.txt").touch()
        (tmp_path / "dir").mkdir()
        (tmp_path / "dir" / "b.txt").touch()
        (tmp_path / "dir" / "sub").mkdir()
        (tmp_path / "dir" / "sub" / "c.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("**", fs_func, recursive=True)
        assert len(result) >= 3

    async def test_glob_double_star_in_middle(self, tmp_path):
        """Test ** in the middle of pattern."""
        (tmp_path / "root").mkdir()
        (tmp_path / "root" / "a.txt").touch()
        (tmp_path / "root" / "sub").mkdir()
        (tmp_path / "root" / "sub" / "b.txt").touch()
        (tmp_path / "root" / "sub" / "deep").mkdir()
        (tmp_path / "root" / "sub" / "deep" / "c.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("root/**/*.txt", fs_func, recursive=True)
        assert len(result) >= 1


class TestGlobIsHidden:
    """Tests for hidden file handling."""

    async def test_hidden_not_matched_by_star(self, tmp_path):
        """Test that .hidden files are not matched by *."""
        (tmp_path / ".hidden").touch()
        (tmp_path / ".config").touch()
        (tmp_path / "visible").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("*", fs_func)
        assert ".hidden" not in result
        assert ".config" not in result
        assert "visible" in result

    async def test_hidden_matched_explicitly(self, tmp_path):
        """Test that hidden files can be matched with explicit dot."""
        (tmp_path / ".hidden").touch()
        (tmp_path / ".config").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob(".*", fs_func)
        assert ".hidden" in result
        assert ".config" in result


class TestGlobSpecialCases:
    """Tests for special glob cases."""

    async def test_glob_with_glob0_path(self, tmp_path):
        """Test _glob0 path with literal basename."""
        (tmp_path / "dir").mkdir()
        (tmp_path / "dir" / "specific.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("dir/specific.txt", fs_func)
        assert result == ["dir/specific.txt"]

    async def test_glob_directory_trailing_slash_exists(self, tmp_path):
        """Test pattern ending with / for existing directory."""
        (tmp_path / "mydir").mkdir()
        (tmp_path / "mydir" / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("mydir/", fs_func)
        assert result == ["mydir/"]

    async def test_glob_directory_trailing_slash_not_exists(self, tmp_path):
        """Test pattern ending with / for non-existing directory."""
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("nonexistent/", fs_func)
        assert result == []

    async def test_glob_with_magic_in_dirname_only(self, tmp_path):
        """Test pattern with magic only in directory part."""
        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir2").mkdir()
        (tmp_path / "dir1" / "file.txt").touch()
        (tmp_path / "dir2" / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("dir?/file.txt", fs_func)
        assert len(result) == 2

    async def test_glob0_empty_basename(self, tmp_path):
        """Test _glob0 with empty basename (trailing slash)."""
        (tmp_path / "mydir").mkdir()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("mydir/", fs_func)
        assert "mydir/" in result


class TestFindSuffix:
    """Tests for _find_suffix function."""

    def test_find_suffix_with_slash(self):
        """Test suffix finding with slash separator."""
        paths = ["/a/b/c/file.txt", "/a/x/c/file.txt"]
        result = globlize(paths)
        # Should find common suffix /c/file.txt
        assert "file.txt" in result

    def test_find_suffix_with_dot(self):
        """Test suffix finding with dot separator."""
        paths = ["prefix.a.suffix.txt", "prefix.b.suffix.txt"]
        result = globlize(paths)
        assert "{" in result

    def test_find_suffix_no_common(self):
        """Test when there's no common suffix."""
        paths = ["/a/b/c", "/x/y/z"]
        result = globlize(paths)
        assert "{" in result

    def test_find_suffix_different_lengths(self):
        """Test suffix finding with different path lengths."""
        paths = ["/a/b/file.txt", "/a/c/d/file.txt"]
        result = globlize(paths)
        # Should still work with different depths
        assert "file.txt" in result


class TestGlobWithNonExistentDir:
    """Test glob behavior when directories don't exist."""

    async def test_iglob_nonexistent_base(self, tmp_path):
        """Test iglob when base directory doesn't exist."""
        fs_func = create_fs_func(str(tmp_path))
        result = []
        async for item in iglob("nonexistent/*.txt", fs_func):
            result.append(item)
        assert result == []

    async def test_glob_magic_dirname_not_exists(self, tmp_path):
        """Test glob when magic directory pattern doesn't match anything."""
        (tmp_path / "other").mkdir()
        (tmp_path / "other" / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("missing*/file.txt", fs_func)
        assert result == []


class TestGlobMagicDirname:
    """Tests for glob with magic patterns in dirname."""

    async def test_glob_star_dirname_with_literal_basename(self, tmp_path):
        """Test * in dirname with literal filename."""
        (tmp_path / "foo").mkdir()
        (tmp_path / "bar").mkdir()
        (tmp_path / "foo" / "target.txt").touch()
        (tmp_path / "bar" / "target.txt").touch()
        (tmp_path / "bar" / "other.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("*/target.txt", fs_func)
        assert len(result) == 2
        assert "foo/target.txt" in result
        assert "bar/target.txt" in result

    async def test_glob_bracket_dirname(self, tmp_path):
        """Test bracket pattern in dirname."""
        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir2").mkdir()
        (tmp_path / "dir3").mkdir()
        (tmp_path / "dir1" / "file.txt").touch()
        (tmp_path / "dir2" / "file.txt").touch()
        (tmp_path / "dir3" / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("dir[12]/file.txt", fs_func)
        assert len(result) == 2
        assert "dir1/file.txt" in result
        assert "dir2/file.txt" in result
        assert "dir3/file.txt" not in result


class TestUngloblizeEscapedBrace:
    """Tests for ungloblize with escaped braces."""

    def test_ungloblize_with_escaped_brace_in_path(self):
        """Test that [{{] pattern is handled in ungloblize loop."""
        # Create a pattern where [{{] appears before a real brace
        result = ungloblize("[{]literal{a,b}")
        # The [{{] should be skipped, and {a,b} should be expanded
        assert len(result) == 2

    def test_ungloblize_multiple_escaped_braces(self):
        """Test multiple escaped braces."""
        # Pattern with escaped brace followed by real brace
        result = ungloblize("pre[{]x{1,2}post")
        assert len(result) == 2


class TestGlob0DirectoryMatch:
    """Tests for _glob0 directory matching."""

    async def test_glob0_with_empty_basename_isdir(self, tmp_path):
        """Test _glob0 when basename is empty and dirname is a directory."""
        (tmp_path / "mydir").mkdir()
        (tmp_path / "mydir" / "file.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("mydir/", fs_func)
        assert "mydir/" in result

    async def test_glob0_with_empty_basename_not_dir(self, tmp_path):
        """Test _glob0 when basename is empty but path is not a directory."""
        (tmp_path / "afile").touch()  # This is a file, not a directory
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("afile/", fs_func)
        # Should not match because afile is not a directory
        assert result == []

    async def test_glob0_with_nonexistent_basename(self, tmp_path):
        """Test _glob0 when basename doesn't exist."""
        (tmp_path / "mydir").mkdir()
        fs_func = create_fs_func(str(tmp_path))
        # Try to match a specific file that doesn't exist
        result = await glob("mydir/nonexistent.txt", fs_func)
        assert result == []


class TestGlobRecursiveDoubleStarOnly:
    """Tests for ** as the only pattern component."""

    async def test_glob_double_star_only(self, tmp_path):
        """Test ** as the entire pattern with recursive=True."""
        (tmp_path / "a.txt").touch()
        (tmp_path / "b.txt").touch()
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "c.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("**", fs_func, recursive=True)
        # Should match all files recursively
        assert len(result) >= 3


class TestGlobHiddenPatterns:
    """Tests for hidden file pattern matching."""

    async def test_glob_hidden_pattern_explicit(self, tmp_path):
        """Test that hidden pattern matches hidden files."""
        (tmp_path / ".gitignore").touch()
        (tmp_path / ".env").touch()
        (tmp_path / "readme.md").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob(".git*", fs_func)
        assert ".gitignore" in result
        assert ".env" not in result

    async def test_glob_all_hidden(self, tmp_path):
        """Test .* pattern matches all hidden files."""
        (tmp_path / ".a").touch()
        (tmp_path / ".b").touch()
        (tmp_path / ".c").touch()
        (tmp_path / "visible").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob(".*", fs_func)
        assert len(result) == 3
        assert "visible" not in result


class TestGlobInternalErrors:
    """Tests for internal error paths - these are defensive checks."""

    async def test_glob2_called_with_recursive_pattern(self, tmp_path):
        """Test _glob2 is called correctly with ** pattern."""
        (tmp_path / "dir").mkdir()
        (tmp_path / "dir" / "file.txt").touch()
        (tmp_path / "dir" / "sub").mkdir()
        (tmp_path / "dir" / "sub" / "deep.txt").touch()
        fs_func = create_fs_func(str(tmp_path))
        result = await glob("dir/**", fs_func, recursive=True)
        # Should work without raising error
        assert len(result) >= 2

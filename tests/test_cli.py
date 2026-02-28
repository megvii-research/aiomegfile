from click.testing import CliRunner

from aiomegfile.__version__ import __version__
from aiomegfile.cli import cli


def test_cli_version():
    """Test the version command prints the package version."""
    runner = CliRunner()
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.output


def test_cli_ls_cat_head_tail(tmp_path):
    """Test basic read-only CLI commands on local files."""
    sample_path = tmp_path / "sample.txt"
    sample_path.write_text("line1\nline2\nline3")

    runner = CliRunner()

    result = runner.invoke(cli, ["ls", str(tmp_path)])
    assert result.exit_code == 0
    assert "sample.txt" in result.output

    result = runner.invoke(cli, ["cat", str(sample_path)])
    assert result.exit_code == 0
    assert "line1" in result.output

    result = runner.invoke(cli, ["head", "-n", "1", str(sample_path)])
    assert result.exit_code == 0
    assert result.output.strip() == "line1"

    result = runner.invoke(cli, ["tail", "-n", "1", str(sample_path)])
    assert result.exit_code == 0
    assert result.output.strip() == "line3"


def test_cli_cp_mv_rm(tmp_path):
    """Test copy, move, and remove commands."""
    src_path = tmp_path / "src.txt"
    src_path.write_text("data")

    dst_dir = tmp_path / "dst"
    dst_dir.mkdir()

    runner = CliRunner()

    result = runner.invoke(cli, ["cp", str(src_path), str(dst_dir)])
    assert result.exit_code == 0
    copied_path = dst_dir / "src.txt"
    assert copied_path.read_text() == "data"

    moved_path = tmp_path / "moved.txt"
    result = runner.invoke(cli, ["mv", str(copied_path), str(moved_path)])
    assert result.exit_code == 0
    assert moved_path.read_text() == "data"
    assert not copied_path.exists()

    result = runner.invoke(cli, ["rm", str(moved_path)])
    assert result.exit_code == 0
    assert not moved_path.exists()

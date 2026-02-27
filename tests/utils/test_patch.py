import sys
import types

from aiomegfile.utils.patch import patch_megfile_smart_methods


def _install_fake_megfile(monkeypatch):
    """Install a fake megfile package into ``sys.modules``.

    :param monkeypatch: Pytest monkeypatch fixture.
    :return: Tuple of (megfile module, megfile.smart module).
    :rtype: tuple
    """
    megfile = types.ModuleType("megfile")
    megfile.__path__ = []
    meg_smart = types.ModuleType("megfile.smart")

    def smart_exists(path):
        """Placeholder smart_exists implementation.

        :param path: Input path.
        :return: Sentinel string for the placeholder implementation.
        :rtype: str
        """
        return f"original:{path}"

    def smart_open(path, mode="r", **kwargs):
        """Placeholder smart_open implementation.

        :param path: Input path.
        :param mode: File mode.
        :param kwargs: Additional keyword arguments.
        :return: Sentinel string for the placeholder implementation.
        :rtype: str
        """
        return f"original:{path}:{mode}:{kwargs}"

    def smart_scandir(path):
        """Placeholder smart_scandir implementation.

        :param path: Input path.
        :return: Sentinel string for the placeholder implementation.
        :rtype: str
        """
        return f"original:{path}"

    meg_smart.smart_exists = smart_exists
    meg_smart.smart_open = smart_open
    meg_smart.smart_scandir = smart_scandir

    megfile.smart = meg_smart
    megfile.smart_exists = smart_exists
    megfile.smart_open = smart_open
    megfile.smart_scandir = smart_scandir

    monkeypatch.setitem(sys.modules, "megfile", megfile)
    monkeypatch.setitem(sys.modules, "megfile.smart", meg_smart)
    return megfile, meg_smart


def test_patch_megfile_smart_exists(tmp_path, monkeypatch):
    """Replace megfile smart_exists with aiomegfile's implementation.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patched = patch_megfile_smart_methods()
    assert "smart_exists" in patched

    file_path = tmp_path / "exists.txt"
    file_path.write_text("ok", encoding="utf-8")

    import megfile.smart as meg_smart

    assert meg_smart.smart_exists(str(file_path)) is True


def test_patch_megfile_smart_open(tmp_path, monkeypatch):
    """Expose a sync smart_open that works with context managers.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patch_megfile_smart_methods()

    import megfile.smart as meg_smart

    file_path = tmp_path / "data.txt"
    with meg_smart.smart_open(str(file_path), "w") as handle:
        handle.write("hello")

    with meg_smart.smart_open(str(file_path), "r") as handle:
        assert handle.read() == "hello"


def test_patch_megfile_smart_scandir(tmp_path, monkeypatch):
    """Expose a sync scandir iterator.

    :param tmp_path: Pytest temporary path fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    _install_fake_megfile(monkeypatch)

    patch_megfile_smart_methods()

    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    (tmp_path / "b.txt").write_text("b", encoding="utf-8")

    import megfile.smart as meg_smart

    entries = list(meg_smart.smart_scandir(str(tmp_path)))
    names = sorted(entry.name for entry in entries)
    assert names == ["a.txt", "b.txt"]

    with meg_smart.smart_scandir(str(tmp_path)) as iterator:
        names = sorted(entry.name for entry in iterator)
        assert names == ["a.txt", "b.txt"]

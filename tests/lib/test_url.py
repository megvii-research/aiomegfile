from aiomegfile.lib.url import fspath, split_uri


def test_fspath_with_bytes():
    assert fspath(b"/tmp/test") == "/tmp/test"


def test_split_uri_without_protocol():
    protocol, path, profile = split_uri("/tmp/path")
    assert protocol == "file"
    assert path == "/tmp/path"
    assert profile is None


def test_split_uri_with_profile():
    protocol, path, profile = split_uri("s3+dev://bucket/key")
    assert protocol == "s3"
    assert path == "bucket/key"
    assert profile == "dev"

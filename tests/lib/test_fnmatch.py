import re

from aiomegfile.lib.fnmatch import filter as fn_filter
from aiomegfile.lib.fnmatch import fnmatch, fnmatchcase, translate


def test_double_star_spans_directories():
    assert fnmatch("a/b/c.txt", "**/*.txt")
    assert fnmatch("root/child/grand/file.md", "root/**/file.md")
    assert not fnmatch("root/child/grand/other.md", "root/**/file.md")


def test_curly_braces_alternation():
    assert fnmatch("foo", "{foo,bar}")
    assert fnmatch("bar", "{foo,bar}")
    assert not fnmatch("baz", "{foo,bar}")


def test_question_and_bracket_patterns():
    assert fnmatch("file1.txt", "file?.txt")
    # unmatched '[' should be treated literally
    regex = translate("note[")
    assert re.match(regex, "note[")
    assert re.match(regex, "note]") is None


def test_filter_returns_matching_subset():
    names = ["a.txt", "b.log", "c.txt"]
    assert fn_filter(names, "*.txt") == ["a.txt", "c.txt"]


def test_fnmatchcase_respects_case():
    assert fnmatchcase("File.TXT", "File.TXT")
    assert not fnmatchcase("File.TXT", "file.txt")


def test_additional_translate_branches():
    # "**" not surrounded by slashes should act like a greedy match (".*")
    assert fnmatch("foo/bar/baz", "foo**baz")
    assert fnmatch("foobaz", "foo**baz")

    # Bracket patterns: negation with "!" and leading "]" and "^" cases
    assert fnmatch("b", "[!a]")
    assert not fnmatch("a", "[!a]")
    assert fnmatch("]", "[]]")
    assert fnmatch("^", "[^a]")
    assert fnmatch("a", "[^a]")
    assert not fnmatch("b", "[^a]")

    # Empty curly braces are treated literally
    assert fnmatch("{}", "{}")
    assert not fnmatch("foo", "{}")
    assert fnmatch("a", "{a,b}")
    assert fnmatch("b", "{a,b}")
    assert not fnmatch("c", "{a,b}")

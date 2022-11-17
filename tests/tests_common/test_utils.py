from radarpipeline.common.utils import is_valid_github_path


def test_is_valid_github_path():
    assert is_valid_github_path("https://github.com/RADAR-base/radarpipeline") is True

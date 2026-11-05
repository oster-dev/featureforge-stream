from importlib.metadata import version

import featureforge_stream


def test_package_is_importable() -> None:
    assert featureforge_stream.__name__ == "featureforge_stream"


def test_installed_package_version() -> None:
    assert version("featureforge-stream") == "0.0.1"

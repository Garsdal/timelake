import os
import shutil
from pathlib import Path

import pytest

from timelake.storage import LocalTimeLakeStorage, S3TimeLakeStorage

TEST_PATH = Path("./timelake_storage_test")
S3_TEST_PATH = "s3://test-bucket/timelake_storage_test"


@pytest.fixture(scope="module")
def local_storage():
    # Setup: clean directory before test
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)

    storage = LocalTimeLakeStorage(TEST_PATH)
    storage.ensure_directories()

    yield storage  # Yield to the test functions

    # Teardown: clean up after test
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)


@pytest.fixture(scope="module")
def s3_storage():
    # Placeholder: Mock S3 setup
    storage = S3TimeLakeStorage(
        S3_TEST_PATH,
    )
    storage.ensure_directories()

    yield storage  # Yield to the test functions

    # Placeholder: Mock S3 teardown
    pass


def test_ensure_directories_local(local_storage: LocalTimeLakeStorage):
    assert os.path.exists(local_storage.path)


def test_ensure_directories_s3(s3_storage: S3TimeLakeStorage):
    # Placeholder: Validate S3 directories (mocked for now)
    assert s3_storage.path == S3_TEST_PATH


def test_get_storage_options_local(local_storage: LocalTimeLakeStorage):
    options = local_storage.get_storage_options()
    assert options == {}

from unittest.mock import patch

import pytest
from prefect.testing.utilities import prefect_test_harness

from etl.hello_world import hello_world


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


def test_hello_world_flow_default_args():
    with patch('etl.hello_world.print_hello') as mock_print_hello:
        with patch('etl.hello_world.print_goodbye') as mock_print_goodbye:
            hello_world()
            mock_print_hello.assert_called_once_with("world")
            mock_print_goodbye.assert_not_called()


def test_hello_world_flow_with_name():
    with patch('etl.hello_world.print_hello') as mock_print_hello:
        with patch('etl.hello_world.print_goodbye') as mock_print_goodbye:
            hello_world("Michelle")
            mock_print_hello.assert_called_once_with("Michelle")
            mock_print_goodbye.assert_not_called()


def test_hello_world_flow_with_name_and_goodbye():
    with patch('etl.hello_world.print_hello') as mock_print_hello:
        with patch('etl.hello_world.print_goodbye') as mock_print_goodbye:
            hello_world("Michelle", True)
            mock_print_hello.assert_called_once_with("Michelle")
            mock_print_goodbye.assert_called_once_with("Michelle")

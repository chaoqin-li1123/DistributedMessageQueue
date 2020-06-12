from test_utils import Swarm, Node

import pytest
import time
import requests

TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"
PROGRAM_FILE_PATH = "../src/node.py"
ELECTION_TIMEOUT = 2.0


@pytest.fixture
def node_with_test_topic():
    node = Swarm(PROGRAM_FILE_PATH, 1)[0]
    node.start(ELECTION_TIMEOUT)
    node.wait_for_flask_startup()
    assert(node.create_topic(TEST_TOPIC).json() == {"success": True})
    yield node
    node.clean()



# test whether is pop in FIFO order
def test_get_message_fifo(node_with_test_topic):
    for i in range(5):
        message = TEST_MESSAGE + str(i)
        assert (node_with_test_topic.put_message(
            TEST_TOPIC, message).json() == {"success": True})
    for i in range(5):
        message = TEST_MESSAGE + str(i)
        assert (node_with_test_topic.get_message(
            TEST_TOPIC).json() == {"success": True, "message": message})


# put the same message into the same topic multiple times, should succeed
def test_put_and_get_same_message(node_with_test_topic):
    for i in range(10):
        assert(node_with_test_topic.put_message(
            TEST_TOPIC, TEST_MESSAGE).json() == {"success": True})
    for i in range(10):
        assert(node_with_test_topic.get_message(
            TEST_TOPIC).json() == {"success": True, "message": TEST_MESSAGE})

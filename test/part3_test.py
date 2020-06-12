from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests

NUM_NODES_ARRAY = [5, 7]
PROGRAM_FILE_PATH = "src/node.py"
TEST_TOPIC = "test_topic"
TEST_MESSAGE = "Test Message"

ELECTION_TIMEOUT = 2.0
NUMBER_OF_LOOP_FOR_SEARCHING_LEADER = 3


@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()

# create the same topic in different term, should only succeed once
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_same_topic_created_in_different_terms(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})

    leader1.clean(ELECTION_TIMEOUT)

    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader2 != None)
    assert (leader2.create_topic(TEST_TOPIC).json() == {"success": False})


# put n messages in first term, get n messages in second term, should always succeed
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_put_and_get_multiple_messages_in_different_terms(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 != None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    for i in range(5):
        assert(leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
               == {"success": True})


    leader1.clean(ELECTION_TIMEOUT)

    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader2 != None)
    for i in range(5):
        assert(leader2.get_message(TEST_TOPIC).json()
               == {"success": True, "message": TEST_MESSAGE})


# put a message in first term, kill 2 leaders, get the message again
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_put_and_get_message_2_node_failures(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)

    assert (leader1 is not None)
    assert (leader1.create_topic(TEST_TOPIC).json() == {"success": True})
    assert (leader1.put_message(TEST_TOPIC, TEST_MESSAGE).json()
                == {"success": True})

    leader1.clean(ELECTION_TIMEOUT)

    leader2 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader2 is not None)
    leader2.clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(NUMBER_OF_LOOP_FOR_SEARCHING_LEADER)
    assert (leader3 is not None)
    assert (leader3.get_message(TEST_TOPIC).json()
                == {"success": True, "message": TEST_MESSAGE})


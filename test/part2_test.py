from test_utils import Swarm, Node, LEADER, FOLLOWER, CANDIDATE
import pytest
import time
import requests

# seconds the program will wait after starting a node for election to happen
# it is set conservatively, you will likely be able to lower it for faster tessting
ELECTION_TIMEOUT = 2.0

# array of numbr of nodes spawned on tests, an example could be [3,5,7,11,...]
# default is only 5 for faster tests
NUM_NODES_ARRAY = [5, 7]


# yoour `node.py` file path
PROGRAM_FILE_PATH = "src/node.py"

TEST_TOPIC = "test_topic"

@pytest.fixture
def swarm(num_nodes):
    swarm = Swarm(PROGRAM_FILE_PATH, num_nodes)
    swarm.start(ELECTION_TIMEOUT)
    yield swarm
    swarm.clean()


# all requests to client will be replied with failure.
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_client_reply_with_failure(swarm: Swarm, num_nodes: int):
    leader = swarm.get_leader_loop(3)
    for node in swarm:
        if node != leader:
            node.create_topic(TEST_TOPIC).json() == {"success": False}
    node.create_topic(TEST_TOPIC).json() == {"success": True}


# kill 2 leaders in a grid of 5 nodes, the 3rd leader should be elected.
@pytest.mark.parametrize('num_nodes', NUM_NODES_ARRAY)
def test_2_failure_in_5_nodes(swarm: Swarm, num_nodes: int):
    leader1 = swarm.get_leader_loop(3)
    assert (leader1 != None)
    leader1.clean(ELECTION_TIMEOUT)
    leader2 = swarm.get_leader_loop(3)
    assert (leader2 != None)
    assert (leader2 != leader1)
    leader2.clean(ELECTION_TIMEOUT)
    leader3 = swarm.get_leader_loop(3)
    assert (leader3 != None)
    assert (leader2 != leader3)

from flask import Flask
import requests
from random import randint
from flask import request, jsonify
import json
import sys
from time import time, sleep
from queue import Queue
import concurrent.futures
import threading

app = Flask(__name__)

# state for this instance
'''
log entry format
[{"term": int, command": string(PUSH/POP), "msg_type": string(TOPIC/MESSAGE), "body": json-formatted String}]
'''
logs = []
LEADER, FOLLOWER, CANDIDATE = 1, 2, 3
status = FOLLOWER
cur_term = 0
node_index = None

# state for all the machines
nodes = []
node_num = 0
commitIndex = 0
lastApplied = 0

# message queue
Queues = dict()
Topics = []

# state for leaders
nextIndex = []
matchIndex = []

# state for election
latest_heartbeat = 0
time_out_const = None
latest_election = 0
vote_for = dict()
heartbeat_timeout = 200
election_timeout = 200
wake_up_interval = 0.03

@app.route('/topic', methods=['PUT'])
def put_topic():
    '''
    create a topic
    '''
    if status != LEADER:
        return jsonify({'success': False})  # request to follower should return failure

    req_body = request.get_data(as_text=True)
    try:
        data = json.loads(req_body)
        topic = str(data['topic'])

        # append this command into the log
        index = append_log("PUSH", "topic", req_body)

        # send append entry call to servers
        replicate_succ = send_signal()

        if replicate_succ == 1:
            send_signal(leader_commit=index)  # send commit message to servers
            response = {'success': put_topic_impl(topic)}
            return jsonify(response), 201
        else:
            response = {'success': False}
            return jsonify(response), 400
    except KeyError:
        response = {'success': False}
        return jsonify(response), 400


@app.route('/topic', methods=['GET'])
def get_topics():
    if status != LEADER:
        return jsonify({'success': False})  # request to follower should return failure
    # obtain the currently existed topics
    topics = get_topics_impl()
    # generate response
    response = {'success': True, 'topics': topics}
    return jsonify(response), 201


@app.route('/message', methods=['PUT'])
def put_message():
    if status != LEADER:
        return jsonify({'success': False})  # request to follower should return failure
    req_body = request.get_data(as_text=True)
    try:
        data = json.loads(req_body)
        topic = str(data['topic'])
        message = str(data['message'])

        # append this command into the log
        index = append_log("PUSH", "message", req_body)

        # send append entry call to servers
        replicate_succ = send_signal()

        if replicate_succ == 1:
            send_signal(leader_commit=index)
            response = {'success': put_message_impl(topic, message)}
            return jsonify(response), 201
        else:
            response = {'success': False}
            return jsonify(response), 400
    except KeyError:
        response = {'success': False}
    return jsonify(response), 400


@app.route('/message/<topic>', methods=['GET'])
def get_message(topic):
    if status != LEADER:
        return jsonify({'success': False}), 300  # request to follower should return failure
    index = len(logs) + 1
    # update the logs
    req_body = json.dumps({"topic": topic})
    index = append_log("POP", "message", req_body)
    # send append entry call to servers
    replicate_succ = send_signal()
    # generate response
    if replicate_succ == 1:
        # obtain the message
        send_signal(leader_commit=index)
        message = get_message_impl(topic)

        if message is None:
            response = {'success': False}
            return jsonify(response), 400
        response = {'success': True, 'message': message}
        return jsonify(response), 201
    else:
        response = {'success': False}
        return jsonify(response), 400


'''
receive the message from leader including append entry, consistency check
'''
@app.route('/appendentry', methods=['GET'])
def on_append_entry():
    global latest_heartbeat, status, cur_term, commitIndex, lastApplied, logs
    latest_heartbeat = get_timestamp_in_ms()
    # extract info
    req_body = request.get_data(as_text=True)
    data = json.loads(req_body)
    term = int(data['term'])
    prevLogIndex = int(data['prevLogIndex'])
    prevLogTerm = int(data['prevLogTerm'])
    leader_commit = int(data['leaderCommit'])
    entries = json.loads(data['entries'])

    # consistency check
    consistent = check_consistency(prevLogIndex, prevLogTerm, entries)
    if consistent == 0:
        return jsonify({"status": "fail"})

    # commit if needed
    if leader_commit != 0 and leader_commit > lastApplied and leader_commit <= len(logs):
        commitIndex = min(leader_commit, len(logs))  # update commit status

        while lastApplied < commitIndex:
            lastApplied += 1
            execute_command(lastApplied)  # update state machine
        return jsonify({"status": "commit"})
    else:
        return jsonify({"status": "success"})

'''
Receive the heartbeat from the leader
'''
@app.route('/heartbeat', methods=['GET'])
def on_heartbeat():
    global latest_heartbeat, status, cur_term
    latest_heartbeat = get_timestamp_in_ms()
    status = FOLLOWER # transit to follower when receiving a heartbeat
    req_body = request.get_data(as_text=True)
    data = json.loads(req_body)
    term = int(data['term'])
    cur_term = term # update term
    return jsonify({"alive": True})


@app.route('/vote', methods=['GET'])
def vote():
    global latest_election
    req_body = request.get_data(as_text=True)
    latest_election = get_timestamp_in_ms()
    try:
        data = json.loads(req_body)
        candidate = int(data['candidate'])
        term = int(data['term'])
        log_index = int(data['log_index'])
        response = vote_impl(term, candidate, log_index)
        return jsonify(response)
    except KeyError:
        response = {'vote_for': -1}
        return jsonify(response)


@app.route('/status', methods=['GET'])
def get_status():
    global status, cur_term
    status_str = "Candidate"
    if status == FOLLOWER:
        status_str = 'Follower'
    elif status == LEADER:
        status_str = 'Leader'
    return jsonify({'role': status_str, 'term': cur_term})


# -------------------------------------------------------------
# functions manipulating logs
def append_log(command, msg_type, body):
    ''' append the command into the logs'''
    index = len(logs)
    log_entry = generate_log(command, msg_type, body)
    while len(logs) <= index:
        logs.append({})
    logs[index] = log_entry
    return index + 1


def update_log(index, entry):
    '''
    during consistency checks, update the log entry to be consistent with the leader
    '''
    global logs
    while len(logs) <= index:
        logs.append({})
    logs[index] = entry
    logs = logs[0:index + 1]


def generate_log(command, msg_type, body):
    '''generate valid log entry based on provided info '''
    log = {
        "term": cur_term,
        "command": command,
        "msg_type": msg_type,
        "body": body
    }
    return log


def check_consistency(prevLogIndex, prevLogTerm, entries):
    '''
    check whether the log entry in prevLogIndex is consistent with the leader
    :return: 1 if consistent
             0 otherwise
    '''
    global logs
    if prevLogIndex == 0:
        if entries != None:
            update_log(prevLogIndex, entries)
    elif prevLogIndex <= len(logs) and logs[prevLogIndex - 1]['term'] == prevLogTerm:
        # if consistent, append/overwite the next log entry
        if entries != None:
            update_log(prevLogIndex, entries)
    elif prevLogIndex != 0:
        return 0
    return 1


# -------------------------------------------------------------
# functions relate to election algorithm

def vote_impl(term, candidate, log_index):
    global vote_for, cur_term, status, latest_heartbeat
    if status == CANDIDATE:
	# If the requestor has smaller term, don't grant vote, otherwise grant vote and become a follower.
        if term < cur_term:
            return {'vote_for': -1}
        else:
            status = FOLLOWER
            latest_heartbeat = get_timestamp_in_ms()
    res = -1
    if term not in vote_for.keys() and term >= cur_term: # has vote in this term been granted?
        if term > cur_term or log_index >= len(logs): # If the requestor has greater term or greater index, grant vote.
            vote_for[term] = candidate
            res = candidate
            # cur_term = term
    return {'vote_for': res}


def collect_vote(i):
    # request vote from i_th node
    data = {'candidate': node_index, 'term': cur_term, "log_index": len(logs)}
    response = request.get('{}:{}/vote'.format(nodes[i][0], nodes[i][1]), data)
    response_data = json.loads(response)
    return int(response_data['vote_for'])


def start_election():
    global nodes, node_index, status, latest_election, cur_term, vote_for
    next_term = cur_term + 1
    if next_term in vote_for.keys():
        return
    status = CANDIDATE
    cur_term = next_term
    vote_for[cur_term] = node_index
    latest_election = get_timestamp_in_ms()
    vote_total = 1
    vote_cnt = 1  # vote for itself
    future_votes = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(len(nodes)):
            if i == node_index:
                continue
            future_votes.append(executor.submit(collect_vote, i))
        for future_vote in concurrent.futures.as_completed(future_votes):
            try:
                this_vote = future_vote.result()
                vote_total += 1
                if this_vote == node_index:
                    vote_cnt += 1
            except:
                continue
    if vote_cnt * 2 > vote_total:
        status = LEADER
        initialize_status()


# -------------------------------------------------------------
# Functions of intercommunication

def send_signal(leader_commit=0):
    '''
    send append entry signal to every node except the leader itself
    is called when heartbeats and leader requests servers to replicate log entries
    :return: 1 if the majority of the server have replicated the log entry (if no need to replicate the log,
                always return 1)
             0 otherwise
    '''
    global nodes, node_index, nextIndex, matchIndex

    signals = []
    repl_cnt = 1
    repl_total = 1

    # append log, or check consistency
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i in range(len(nodes)):
            if i == node_index:
                continue

            signals.append(executor.submit(signal_func, i, leader_commit))

        for signal in concurrent.futures.as_completed(signals):
            try:
                this_vote = signals.result()
                repl_total += 1
                if this_vote == 1:
                    repl_cnt += 1
            except:
                continue

    if repl_cnt * 2 > repl_total:
        return 1
    else:
        return 0


def signal_func(i, leader_commit):
    '''
    works like AppendEntry RPC, includes functionalities of
    1. heartbeat:  when the enties is None
    2. commit confirmation: when the leader_commit !=0
    3. consistency check
    '''
    global nodes, node_index, nextIndex, matchIndex

    # send signal until consistent
    while True:
        prevLogIndex = nextIndex[i] - 1
        if nextIndex[i] - 1 >= len(logs):
            entries = None
        else:
            entries = logs[nextIndex[i] - 1]

        # construct communication data
        prevLogTerm = 0 if prevLogIndex > len(logs) or prevLogIndex == 0 else logs[prevLogIndex - 1]['term']
        data = {
            "term": cur_term,
            "prevLogIndex": prevLogIndex,
            "prevLogTerm": prevLogTerm,
            "entries": json.dumps(entries),
            "leaderCommit": leader_commit
        }

        try:
            response = requests.get('{}:{}/appendentry'.format(nodes[i][0], nodes[i][1]), json=data)
        except:
            break  # if the server is down, don't wait for response

        response = json.loads(response.text)
        if response['status'] == "fail":
            nextIndex[i] -= 1  # if inconsistency, decrease the nextindex
        else:
            nextIndex[i] += 1
            nextIndex[i] = min(nextIndex[i], len(logs) + 1)
            matchIndex[i] = prevLogIndex + 1
            break
    return 1

def send_heartbeat():
    '''
    send heartbeat signal to every node except the leader itself
    :return:
    '''
    global nodes, node_index
    data = {"term": cur_term}
    for i in range(len(nodes)):
        if i == node_index:
            continue
        try:
            requests.get('{}:{}/heartbeat'.format(nodes[i][0], nodes[i][1]), json=data)
        except:
            continue

def initialize_status():
    '''
    when a node is elected as leader, initialize the nextIndex and matchIndex lists
    '''
    global nextIndex, matchIndex
    nextIndex = []
    matchIndex = []
    for i in range(node_num):
        nextIndex.append(len(logs) + 1)
        matchIndex.append(0)


def status_monitor():
    # a thread that wake up periodically and decide what this node should do.
    global latest_election, status
    while True:
        sleep(wake_up_interval)
        if status == FOLLOWER:
            if time_out():
                start_election()
            else:
                continue
        elif status == LEADER:
            send_heartbeat()
        elif status == CANDIDATE:
            cur = get_timestamp_in_ms()
            if cur - latest_election < election_timeout:
                continue
            start_election()


def time_out():
    cur = get_timestamp_in_ms()
    return cur - latest_heartbeat > heartbeat_timeout


# -------------------------------------------------
# Functions relate to queue management
def execute_command(index):
    command = logs[index - 1]
    body = json.loads(command['body'])
    topic = body['topic']
    if command['msg_type'] == 'topic':
        if command['command'] == 'PUSH':
            put_topic_impl(topic)
    else:
        if command['command'] == 'PUSH':
            message = body['message']
            put_message_impl(topic, message)
        else:
            get_message_impl(topic)


def put_message_impl(topic, message):
    global Queues
    if topic not in Queues.keys():
        return False
    Queues[topic].put(message)
    return True


def put_topic_impl(topic):
    global Topics, Queues
    if topic not in Queues.keys():
        Queues[topic] = Queue()
        Topics.append(topic)
        return True
    return False


def get_message_impl(topic):
    # return a message if the topic has message, return None otherwise
    global Queues
    if topic not in Queues.keys():
        return None
    if Queues[topic].empty():
        return None
    return Queues[topic].get()


def get_topics_impl():
    # return a list of topic
    global Topics
    return [str(topic) for topic in Topics]


def get_timestamp_in_ms():
    return int(round(time() * 1000))


def get_config(config_pathname):
    global nodes, node_index, node_num
    with open(config_pathname) as config_file:
        config = json.load(config_file)
        addresses = config['addresses']
        for address in addresses:
            nodes.append([address['ip'], address['port']])
    node_num = len(nodes)


def main(argv):
    global node_index, heartbeat_timeout, cur_term, latest_heartbeat, election_timeout, wake_up_interval
    node_index = int(argv[2])
    get_config(argv[1])
    heartbeat_timeout = randint(120, 360)
    election_timeout = randint(80, 360)
    wake_up_interval = randint(20, 50) / 1000
    latest_heartbeat = get_timestamp_in_ms()
    monitor = threading.Thread(target=status_monitor)
    monitor.start()
    app.run(host=nodes[node_index][0][7:], port=nodes[node_index][1], threaded=True)


if __name__ == "__main__":
    main(sys.argv)

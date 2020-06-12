## Part1 MESSAGE QUEUE
### REST API
Define flask route get_message, get_topics, put_topic and put_message,
For each of the REST API, parse the request body and pass parameters to the implementation of these interface:
get_message_impl, get_topics_impl, put_topic_impl and put_message_impl, which return the result to the REST API
### Data Structure for Messages and Topics
Use global variables to store topics and messages
- Queues = dict(): It is a dictionary, whose keys are string of topics and values are FIFO Queue of string of message.
- Topics = []: It is a list that store the topics in the order that they are inserted.
### Start a Message Queue Server
When a node starts, it calls get_config() to parse config.json and store the addresses of all the nodes in the list.
Use the address provided to start a flask APP.

## Part2 ELECTION
### Commucation Between Nodes
For consistency of the API, use REST for the commuciation between nodes. 
- Use /appendentry API to send heartbeat signal.
- Use /vote API to send grand vote request.
### Status Monitor
Start an independent thread to monitor the status of the node. thread status_monitor() is a thread in a infinite while loop, it wakes up every wake_up_interval(a random integer) ms to check the status of the node and decide what to do.
#### Follower Behavior
- Check the last timestamp that a heartbeat was received. 
- If there is no heartbeat timeout, go back to sleep.
- If there is an heartbeat timeout, this node will become a candidate, start an election and collect vote from other nodes. If the majority of alive nodes grant votes to the candidate, it will become leader.
- If an election fails, go back to sleep.
#### Leader Behavior
- Send heartbeat signal to every node in the list. Check the status of the node before responding to a request. Only a leader can respond to put_topic, get_topics, put_message and get_message request. 
#### Candidate Behavior
- Check if there is an election timeout. If yes, start another election, otherwise go back to sleep.
### Timeout Policy
- The timeout constant for heartbeat is random integer between 180ms and 360ms.
- The timeout constant for election is a fixed integer between 80ms and 360ms.
- lastest_heartbeat and latest_election records the timestamp that the lastest heartbeat is received or the latest election starts.
### Election
- Candidates use concurrent.futures.ThreadPoolExecutor() to start multiple threads and send request(CandidateID, term and index) to collect vote.
- Each node use vote_for = dict() as a dictionary to keep track of the node it votes for in each term and make sure that it only votes for 1 candidate in each term.
- Only node with equal or greater term and index than this node will be granted vote. And vote is granted with first-come-first-serve policy. When an vote request comes, first checks whether the vote for this term has been granted, then check whether the candidate has equal or higher term and index.

## Part3 REPLICATION

#### Communication between nodes:

Using REST API

1. /appendentry: leader receive the call to check consistency, replicate the log entry, and commit to the state machine



#### Leader Behavior:

1. maintains a *nextIndex*, prevIndex for each follower
2. Check consistency with the followers, using SendSignal function (when parameter commit = 0)
3. Send the commit confirmation message to the followers, using SendSignal function 
4. When a client's request arrives, leader first fan out the request to replicate the log entry to all the followers, and collect the vote.
   1. If the majority of the followers successfully replicate the log entry, then the leader send the commit confirmation to all the followers, and commit the entry to its own state machine, and respond to the client
   2. If the majority of the followers failed to replicate the log, then the leader will return false to the client.



#### Follower and Candidate Behavior

1. Receives the append entry call from leader, if the preIndex is not matched, return false. And if the preIndex is matched, fit the entries included in the parameters in the NextIndex included in the parameters
2. Receives the commit comfirmatino from leader. increase the commitIndex, and execute the command in the logs.



#### Check Consistency

- Leader send PrevIndex, and PrevTerm to servers
  - if the server rejects, leader will retry by decreasing the prevIndex until the follower matches with the leader
    - When the server finally matches with the leader, then the leader will increase its prevIndex and NextIndex, send the log entry one by one until all the log enties in the server is consistent with the ones in leader
- Other server: check if the log entry in the PrevIndex's term is consistent with the PrevTerm (and whether the log entry's length is larger than PrevIndex), if not, return false, if true, return true, and fill the NextIndex with the entry sent from the leader
- Note: In our implementation, the index of the log entries begins with 1.

### 

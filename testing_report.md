## message_queue_test:

- successfully run for 100 times consecutively

## election_test.py

- successfully run for 100 times consecutively

## replication_test.py

- successfully run for 100 times consecutively

## part1_test

- test_get_message_fifo: test whether is pop in FIFO order.
- test_put_and_get_same_message: put the same message into the sam topic multiple times, should succeed.
## part2_test
- test_client_reply_with_failure: all requests to client will be replied with failure.
- test_2_failure_in_5_nodes: kill 2 leaders in a grid of 5 nodes, the 3rd leader should be elected.
## part3_test
- test_same_topic_created_in_different_terms: create the same topic in different term, should only succeed once
- test_put_and_get_multiple_messages_in_different_terms: put n messages in first term, get messages twice in second term, should always succeed
- test_put_and_get_message_2_node_failures: put a message in first term, kill 2 leaders, get the message again

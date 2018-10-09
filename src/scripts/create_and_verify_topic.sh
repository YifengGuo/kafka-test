# start zookeeper
sudo /usr/local/Cellar/zookeeper/3.4.12/libexec/bin/zkServer.sh start

# install a kafka broker
sudo /usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-server-start.sh -daemon ../config/server.properties

# create and verify a topic called test
sudo /usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

# show the info of a topic
sudo /usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic test

# produce messages to a test topic
sudo /usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

# consume messages from a test topic
sudo /usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
Kafka
sudo apt-get install openjdk-8-jdk

wget https://archive.apache.org/dist/kafka/3.3.1/kafka_2.12-3.3.1.tgz

tar -xzvf kafka_2.12-3.3.1.tgz

cd kafka_2.12-3.3.1

sudo nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

sudo nohup bin/kafka-server-start.sh config/server.properties &

sudo bin/kafka-topics.sh --create --topic ornek --bootstrap-server localhost:9092

sudo bin/kafka-topics.sh --create --topic ornek1 --bootstrap-server localhost:9092

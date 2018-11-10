# Spring Togglz Kafka Example

docker-compose up -d --build

Every instance own consumer group (needs to see every change)
Monitor consumer lag (log compacted featureStateTopic)

* `docker run --net=host --rm confluentinc/cp-kafka:5.0.0 kafka-topics --zookeeper localhost:2128 --create --topic feature-states --replication-factor 1 --partitions 3`
* `docker run --net=host --rm confluentinc/cp-kafka:5.0.0 kafka-configs --zookeeper localhost:2128 --entity-type topics --entity-name feature-states --alter --add-config cleanup.policy=compact,delete.retention.ms=604800000`

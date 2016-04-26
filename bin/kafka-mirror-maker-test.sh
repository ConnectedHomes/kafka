#!/usr/bin/env bash
/Users/sukhdev.saini/gitRepo/kafka_2.10-0.9.0.1/kafka_2.10-0.9.0.0/kafka/bin/kafka-run-class.sh kafka.tools.MirrorMaker\
  --consumer.config /Users/sukhdev.saini/gitRepo/kafka_2.10-0.9.0.1/kafka_2.10-0.9.0.0/kafka/consumerSource.config \
  --producer.config /Users/sukhdev.saini/gitRepo/kafka_2.10-0.9.0.1/kafka_2.10-0.9.0.0/kafka/producerTarget.config \
  --whitelist="upstream" \
  --targetTopicPrefix="targetUpstream" \
  --partitions="1..10"    \
  --new.consumer

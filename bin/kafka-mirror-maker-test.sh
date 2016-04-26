#!/usr/bin/env bash

base_dir=$(dirname $0)/..

$base_dir/bin/kafka-run-class.sh kafka.tools.MirrorMaker\
  --consumer.config $base_dir/config/consumer-source-mirror-maker.properties \
  --producer.config $base_dir/config/producer-target-mirror-maker.properties \
  --whitelist="upstream" \
  --targetTopic="targetUpstream" \
  --partitions="1..10"    \
  --new.consumer
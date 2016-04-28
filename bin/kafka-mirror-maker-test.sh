#!/usr/bin/env bash

base_dir=$(dirname $0)/..

echo "Starting Mirror Maker on $@ environment"
$base_dir/bin/kafka-run-class.sh kafka.tools.MirrorMaker\
  --consumer.config $base_dir/config/$1/consumer-source-mirror-maker.properties \
  --producer.config $base_dir/config/$1/producer-target-mirror-maker.properties \
  --whitelist="upstream" \
  --targetTopicPrefix="prod" \
  --partitions="1..10"    \
  --new.consumer
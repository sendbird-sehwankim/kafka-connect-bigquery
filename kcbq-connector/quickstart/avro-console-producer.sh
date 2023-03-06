#! /usr/bin/env bash
#
# Copyright 2020 Confluent, Inc.
#
# This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

BASE_DIR=`dirname "$0"`

if [ -z $CONFLUENT_DIR ]; then
  CONFLUENT_DIR="$BASE_DIR/../../confluent-3.0.0"
fi

KAFKA_TOPIC='write_api_test22'
#AVRO_SCHEMA='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
AVRO_SCHEMA='{"type":"record","name":"myrecord","fields":[{"name":"f0","type":"string"},{"name":"f1","type":"string"},{"name":"f2","type":"string"},{"name":"f3","type":"string"},{"name":"f4","type":"string"},{"name":"f5","type":"string"},{"name":"f6","type":"string"},{"name":"f7","type":"string"},{"name":"f8","type":"string"},{"name":"f9","type":"string"},{"name":"f10","type":"string"},{"name":"f11","type":"string"},{"name":"f12","type":"string"},{"name":"f13","type":"string"},{"name":"f14","type":"string"},{"name":"f15","type":"string"},{"name":"f16","type":"string"},{"name":"f17","type":"string"},{"name":"f18","type":"string"},{"name":"f19","type":"string"},{"name":"f20","type":"string"},{"name":"f21","type":"string"},{"name":"f22","type":"string"},{"name":"f23","type":"string"},{"name":"f24","type":"string"},{"name":"f25","type":"string"},{"name":"f26","type":"string"},{"name":"f27","type":"string"},{"name":"f28","type":"string"},{"name":"f29","type":"string"},{"name":"f30","type":"string"},{"name":"f31","type":"string"},{"name":"f32","type":"string"},{"name":"f33","type":"string"},{"name":"f34","type":"string"},{"name":"f35","type":"string"},{"name":"f36","type":"string"},{"name":"f37","type":"string"},{"name":"f38","type":"string"},{"name":"f39","type":"string"},{"name":"f40","type":"string"},{"name":"f41","type":"string"},{"name":"f42","type":"string"},{"name":"f43","type":"string"},{"name":"f44","type":"string"},{"name":"f45","type":"string"},{"name":"f46","type":"string"},{"name":"f47","type":"string"},{"name":"f48","type":"string"},{"name":"f49","type":"string"},{"name":"f50","type":"string"},{"name":"f51","type":"string"},{"name":"f52","type":"string"},{"name":"f53","type":"string"},{"name":"f54","type":"string"},{"name":"f55","type":"string"},{"name":"f56","type":"string"},{"name":"f57","type":"string"},{"name":"f58","type":"string"},{"name":"f59","type":"string"},{"name":"f60","type":"string"},{"name":"f61","type":"string"},{"name":"f62","type":"string"},{"name":"f63","type":"string"},{"name":"f64","type":"string"},{"name":"f65","type":"string"},{"name":"f66","type":"string"},{"name":"f67","type":"string"},{"name":"f68","type":"string"},{"name":"f69","type":"string"},{"name":"f70","type":"string"},{"name":"f71","type":"string"},{"name":"f72","type":"string"},{"name":"f73","type":"string"},{"name":"f74","type":"string"},{"name":"f75","type":"string"},{"name":"f76","type":"string"},{"name":"f77","type":"string"},{"name":"f78","type":"string"},{"name":"f79","type":"string"},{"name":"f80","type":"string"},{"name":"f81","type":"string"},{"name":"f82","type":"string"},{"name":"f83","type":"string"},{"name":"f84","type":"string"},{"name":"f85","type":"string"},{"name":"f86","type":"string"},{"name":"f87","type":"string"},{"name":"f88","type":"string"},{"name":"f89","type":"string"},{"name":"f90","type":"string"},{"name":"f91","type":"string"},{"name":"f92","type":"string"},{"name":"f93","type":"string"},{"name":"f94","type":"string"},{"name":"f95","type":"string"},{"name":"f96","type":"string"},{"name":"f97","type":"string"},{"name":"f98","type":"string"},{"name":"f99","type":"string"}]}'

REGISTRY_URL='http://localhost:8081'
BROKER_LIST='localhost:9092'

usage() {
  echo -e "usage:\n" \
          "    [-t|--topic <kafka_topic>]               Name of the kafka topic to write to\n" \
          "    [-s|--schema <avro_schema>]              Avro schema definition\n" \
          "    [-f|--schema-file <avro_schema_file>]    File containing Avro schema definition\n" \
          "    [-r|--registry <registry_url>]           Schema Registry URL to use\n" \
          "    [-b|--broker-list <broker_list>]         Comma-separated list of Kafka brokers\n" \
          "        <kafka_topic> defaults to '$KAFKA_TOPIC'\n" \
          "        <avro_schema> defaults to '$AVRO_SCHEMA'\n" \
          "        <registry_url> defaults to '$REGISTRY_URL'\n" \
          "        <broker_list> defaults to '$BROKER_LIST'"
  exit ${1:-0}
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -h|--help)
      usage ;;
    -t|--topic)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide name of kafka topic following $1 flag"
        usage 1
      else
        shift
        KAFKA_TOPIC="$1"
      fi ;;
    -s|--schema)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide schema following $1 flag"
        usage 1
      else
        shift
        AVRO_SCHEMA="$1"
      fi ;;
    -f|--schema-file)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide schema file following $1 flag"
        usage 1
      else
        shift
        AVRO_SCHEMA=`cat "$1"`
        exit_value=$?
        if [[ $exit_value != 0 ]]; then
          exit $exit_value
        fi
      fi ;;
    -r|--registry-url)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide registry url following $1 flag"
        usage 1
      else
        shift
        REGISTRY_URL="$1"
      fi ;;
    -b|--broker-list)
      if [[ $# -lt 2 ]]; then
        echo "$0: must provide broker list following $1 flag"
        usage 1
      else
        shift
        BROKER_LIST="$1"
      fi ;;
    *)
      echo "$1: unrecognized option"
      usage 1 ;;
  esac
  shift
done
export CONFLUENT_DIR=~/confluent-7.3.0

FILE_NAME='/Users/bhagyashree/Utilities/big_data.txt'
i=0
while true; do
while read line; do
  echo $line
  i=$((i+1))

  done < $FILE_NAME | exec "$CONFLUENT_DIR/bin/kafka-avro-console-producer" \
    --broker-list "$BROKER_LIST" \
    --topic "$KAFKA_TOPIC" \
    --property value.schema="$AVRO_SCHEMA" \
    --property schema.registry.url="$REGISTRY_URL"

    echo "next iteration"
    break
done



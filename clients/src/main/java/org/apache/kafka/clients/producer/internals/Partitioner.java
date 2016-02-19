/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;


/**
 * The default partitioning strategy:
 * <ul>
 * <li>If a partition is specified in the record, use it
 * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
 * <li>If no partition or key is present choose a partition in a round-robin fashion
 */
public class Partitioner {

    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

    /**
     * Multiplicative hashing, in which the hash index is computed as ⌊m * frac(ka)⌋.
     * Here k is again an integer hash code, a is a real number and frac is the function that returns
     * the fractional part of a real number.
     *
     * Multiplicative hashing sets the hash index from the fractional part of multiplying k by a large real number.
     * Multiplicative hashing works well for the same reason that linear congruential multipliers generate apparently
     * random numbers—it's like generating a pseudo-random number with the hashcode as the seed.
     *
     * The multiplier a should be large and its binary representation should be a "random" mix of 1's and 0's.
     * Multiplicative hashing is cheaper than modular hashing because multiplication
     * is usually considerably faster than division (or mod).
     */
    private static Double randomLookingRealNum = Math.sqrt(5);
    private static int multiplicativeHash(int key, int buckets) {
        return (int) Math.floor(buckets * (key * randomLookingRealNum % 1));
    }

    /*
     * Utility methods to show the proper way
     * to convert integers and strings
     * to byte arrays for the purpose of this
     * partitioner
     */
    public byte[] intToBytes(int key) {
        return Integer.toString(Math.abs(key)).getBytes();
    }
    public byte[] stringToBytes(String key) {
        return key.replace("-","").getBytes(); // Note we remove all '-' to avoid negative numbers
    }

    /**
     * Compute the partition for the given record.
     *
     * @param record The record being sent
     * @param cluster The current cluster metadata
     */
    public int partition(ProducerRecord<byte[], byte[]> record, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(record.topic());
        int numPartitions = partitions.size();
        if (record.partition() != null) {
            // they have given us a partition, use it
            if (record.partition() < 0 || record.partition() >= numPartitions)
                throw new IllegalArgumentException("Invalid partition given with record: " + record.partition()
                        + " is not in the range [0..."
                        + numPartitions
                        + "].");
            return record.partition();
        } else if (record.key() == null) {
            int nextValue = counter.getAndIncrement();
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(record.topic());
            if (availablePartitions.size() > 0) {
                int part = Utils.abs(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.abs(nextValue) % numPartitions;
            }
        } else {
            /**
             * hash the key to choose a partition
             * Use BigInteger to convert bytes to int
             * unless the string is numeric in which case
             * we parse the integer.
             **/
            int keyInt;
            String keyString = new String(record.key());
            if (StringUtils.isNumeric(keyString)) {
                keyInt = Integer.parseInt(keyString);
            } else {
                keyInt = new BigInteger(record.key()).intValue();
            }
            return Utils.abs(multiplicativeHash(keyInt,numPartitions));
        }
    }

}
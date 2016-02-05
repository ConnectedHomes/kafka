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

package kafka.producer

import java.util.Properties

// A base producer used whenever we need to have options for both old and new producers;
// this class will be removed once we fully rolled out 0.9
trait BaseProducer {
  def send(topic: String, key: Array[Byte], value: Array[Byte])
  def close()

  private val randomLookingRealNum = Math.sqrt(5)

  def bucket(numOfBuckets: Int, id: Long): Int = {
    Math.floor(numOfBuckets * (id * randomLookingRealNum % 1)).toInt
  }

  def toPartition(partitionNum: Int, key: Array[Byte]): Int = {
    bucket(partitionNum, Binary.fromUInt48(key))
  }
}

class NewShinyProducer(producerProps: Properties) extends BaseProducer {
  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
  import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback

  // decide whether to send synchronously based on producer properties
  val sync = producerProps.getProperty("producer.type", "async").equals("sync")

  val partitionNum = Option(producerProps.getProperty("partition.num")).map(_.toInt)

  val producer = new KafkaProducer[Array[Byte],Array[Byte]](producerProps)

  override def send(topic: String, key: Array[Byte], value: Array[Byte]) {
    val record = partitionNum match {
      case None => new ProducerRecord[Array[Byte],Array[Byte]](topic, key, value)
      case Some(v) => new ProducerRecord[Array[Byte],Array[Byte]](topic, toPartition(v, key), key, value)
    }

    if(sync) {
      this.producer.send(record).get()
    } else {
      this.producer.send(record,
        new ErrorLoggingCallback(topic, key, value, false))
    }
  }

  override def close() {
    this.producer.close()
  }
}

class OldProducer(producerProps: Properties) extends BaseProducer {
  import kafka.producer.{KeyedMessage, ProducerConfig}

  // default to byte array partitioner
  if (producerProps.getProperty("partitioner.class") == null)
    producerProps.setProperty("partitioner.class", classOf[kafka.producer.ByteArrayPartitioner].getName)
  val producer = new kafka.producer.Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))

  override def send(topic: String, key: Array[Byte], value: Array[Byte]) {
    this.producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topic, key, value))
  }

  override def close() {
    this.producer.close()
  }
}

object Binary {

  def fromUInt48(bytes: Array[Byte]): Long =
    (bytes(0) & 0xFFL) << 40 |(bytes(1) & 0xFFL) << 32 |(bytes(2) & 0xFFL) << 24 |(bytes(3) & 0xFFL) << 16 |(bytes(4) & 0xFFL) << 8 |(bytes(5) & 0xFFL)

  def toUInt48(value: Long): Array[Byte] = {
    val bytes = new Array[Byte](6)

    bytes(0) = (( value / ( 1L << 40 ) ) & 0xFFL).asInstanceOf[Byte]
    bytes(1) = (( value / ( 1L << 32 ) ) & 0xFFL).asInstanceOf[Byte]
    bytes(2) = (( value / ( 1L << 24 ) ) & 0xFFL).asInstanceOf[Byte]
    bytes(3) = (( value / ( 1L << 16 ) ) & 0xFFL).asInstanceOf[Byte]
    bytes(4) = (( value / ( 1L << 8 ) ) & 0xFFL).asInstanceOf[Byte]
    bytes(5) = (( value / ( 1L << 0 ) ) & 0xFFL).asInstanceOf[Byte]

    bytes
  }

}


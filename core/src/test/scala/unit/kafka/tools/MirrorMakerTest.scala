package unit.kafka.tools

/**
  * Created by sukhdev.saini on 22/04/2016.
  */

import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

import kafka.tools.{ConsoleConsumer, MessageFormatter}
import kafka.utils.{CoreUtils, Logging, TestUtils, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.junit.{After, Before, Test}
import org.scalatest.{FunSpec, GivenWhenThen, Matchers}


import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

import _root_.kafka.message.MessageAndMetadata
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.DefaultDecoder

import scala.concurrent.duration._
import scala.language.reflectiveCalls
import kafka.tools.MirrorMaker


class MirrorMakerTest extends FunSpec with Matchers with GivenWhenThen  {


  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val part2 = 1
  val tp2 = new TopicPartition(topic, part2)


  describe("KafkaZooKeeperCluster") {

    it("should provide Kafka topics for reading and writing data") {
      Given("a ZooKeeper instance")
      And("a Kafka broker instance")
      And("a Kafka topic")
      val sourceTopicName = "upstream"
      val targetTopicName = "prod_upstream"

      // val cluster =
      //val cluster = new KafkaZooKeeperCluster(topics = Seq(KafkaTopic(topicName)))
      //cluster.start()

      And("some words")
      val words = Seq("terran", "protoss", "zerg")
      val expectedWords = Seq("protoss", "zerg", "terran")
      val arr: Array[String] = Array("--consumer.config", "/Users/sukhdev.saini/gitRepo/kafka_2.10-0.9.0.1/kafka_2.10-0.9.0.0/kafka/consumerSource.config",
        "--producer.config", "/Users/sukhdev.saini/gitRepo/kafka_2.10-0.9.0.1/kafka_2.10-0.9.0.0/kafka/producerTarget.config",
        "--whitelist", "upstream6",
        "--targetTopic", "targetUpstream6",
        "--partitions", "1..100",
        "--new.consumer"
      )

      // Start mirror maker in different window 
      //MirrorMaker.main(arr)

      And("a Kafka producer")
      val producer = {
        val config = {
          val c = new Properties
          c.load(this.getClass.getResourceAsStream("/producer-defaults.properties"))
          c.put("metadata.broker.list", "192.168.99.100:9092")
          c.put("producer.type", "sync")
          c.put("client.id", "test-sync-producer")
          c.put("request.required.acks", "1")
          c
        }
        new Producer[Array[Byte], Array[Byte]](new ProducerConfig(config))
      }

      var zkPort: Int = 2181
      var zkUtils: ZkUtils = null
      val zkConnectionTimeout = 6000
      val zkSessionTimeout = 6000
      def zkConnect: String = "192.168.99.101:" + zkPort

      And("a Kafka consumer")
      // The Kafka consumer must be running before the first messages are being sent to the topic.
      val consumer = new ConsumerApp(zkConnect, targetTopicName)
      val waitForConsumerStartup = 300.millis
      println(s"Waiting $waitForConsumerStartup for consumer threads to launch")
      Thread.sleep(waitForConsumerStartup.toMillis)
      println("Finished waiting for consumer threads to launch")

      When(s"I send the words to the topic")
      words foreach {
        case word =>
          println(s"Synchronously sending word $word to topic $sourceTopicName")
          val msg = new KeyedMessage[Array[Byte], Array[Byte]](sourceTopicName, StringCodec.encode(word))
          producer.send(msg)
      }

      Then("the consumer receives the words")
      val waitForConsumerToReadProducerOutput = 900.millis
      println(s"Waiting $waitForConsumerToReadProducerOutput for consumer threads to read messages")
      Thread.sleep(waitForConsumerToReadProducerOutput.toMillis)
      println("Finished waiting for consumer threads to read messages")
      //if(consumer.receivedWords.equals(words))
      //  println("Send and received words match")
      consumer.receivedWords.toList should equal(expectedWords)

      // Cleanup
      consumer.shutdown()
      //cluster.stop()
    }

  }

}

object StringCodec {

  def encode(s: String): Array[Byte] = s.getBytes

  def decode(bytes: Array[Byte]): String = new String(bytes)

}

class ConsumerApp(val zookeeperConnect: String,
                  val topic: String,
                  val numStreams: Int = 1 /* A single thread ensures we see incoming messages in the correct order. */)
{

  private val receivedMessages = new ConcurrentLinkedQueue[String]
  private val executor = Executors.newFixedThreadPool(numStreams)

  private val consumerConnector = {
    val config = {
      val c = new Properties
      c.load(this.getClass.getResourceAsStream("/consumer-defaults.properties"))
      c.put("zookeeper.connect", zookeeperConnect)
      c
    }
    Consumer.create(new ConsumerConfig(config))
  }

  private val consumerThreads = {
    val consumerMap = {
      val topicCountMap = Map(topic -> numStreams)
      val keyDecoder = new DefaultDecoder
      val valueDecoder = new DefaultDecoder
      consumerConnector.createMessageStreams(topicCountMap, keyDecoder, valueDecoder)
    }
    consumerMap.get(topic) match {
      case Some(streams) => streams.view.zipWithIndex map {
        case (stream, threadId) =>
          new Runnable {
            override def run() {
              stream foreach {
                case msg: MessageAndMetadata[_, _] =>
                  println(s"Consumer thread $threadId received message: " + msg)
                  val word = StringCodec.decode(msg.message())
                  println("Decoded word is: " + word)
                  receivedMessages.add(word)
                case _ => println(s"Received unexpected message type from broker")
              }
            }
          }
      }
      case _ => Seq()
    }
  }
  consumerThreads foreach executor.submit

  def receivedWords: Seq[String] = receivedMessages.toArray.map(_.asInstanceOf[String]).toSeq

  def shutdown() {
    consumerConnector.shutdown()
    executor.shutdown()
  }



}

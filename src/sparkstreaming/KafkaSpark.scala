package sparkstreaming

import java.util.HashMap
import org.apache.spark.SparkContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.log4j.{BasicConfigurator, Level, Logger}

object KafkaSpark {
  def main(args: Array[String]) {

    BasicConfigurator.configure()
    Logger.getRootLogger.setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("streaming").setMaster("local")
    conf.set("spark.cassandra.connection.host", "cassandra")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/tmp")

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("cassandra").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count double);")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = Map(
        "metadata.broker.list" -> "kafka:9092",
        "zookeeper.connect" -> "zookeeper:2181",
        "group.id" -> "kafka-spark-streaming",
        "zookeeper.connection.timeout.ms" -> "1000"
    )
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaConf, Array("avg").toSet
    )
    def splittingFunc(record: (String, String)): (String, Double) = {
        val splitted = record._2.split(",")
        (splitted(0), splitted(1).toDouble)
    }
    val pairs = messages.map(splittingFunc)

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
        val current_state = state.getOption.getOrElse((0.0, 0))
        val old_average = current_state._1
        val old_count = current_state._2
        val new_count = old_count + 1
        val actual_value = value.getOrElse(0.0)
        val new_average = (old_average * old_count / new_count) + (actual_value / new_count)

        state.update((new_average, new_count))
        (key, new_average)
    }
    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()

  }
}

package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage

object ScalaProducerExample extends App {
    def getRandomVal: String = {
    	val i = Random.nextInt(alphabet.size)
        val key = alphabet(i)
        val value = Random.nextInt(alphabet.size)
        key + "," + value
    }

    val alphabet = 'a' to 'z'
    val events = 10000
    val topic = "avg"
    val brokers = "kafka:9092"
    val rnd = new Random()

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    var i = 0
    for (i <- 1 to events) {
        val data = new ProducerRecord[String, String](topic, null, getRandomVal)
        producer.send(data)
        print(data + "\n")
    }

    producer.close()
}

package com.project.demo
import scala.io
import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer {

  def main(args:Array[String]): Unit = {
    writeToKafka("quickstart-events")
    print("main")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val collected=io.Source.fromFile("/home/hariharan/dataset/small_ds").getLines.toList
    
    var arr=List[List[String]]()
    for(line<- collected)
    {
      var a=line.split(",").toList
      arr=arr:+a
      print(arr.length)
      
    }
    var len=arr.length
    for (rows <- 0 to arr.length){
    producer.send(new ProducerRecord[String, String](topic, "name", arr(rows)(0)))
    producer.send(new ProducerRecord[String, String](topic, "marks", arr(rows)(1)))
    }
    val record = new ProducerRecord[String, String](topic, "key", "value")
    producer.send(new ProducerRecord[String, String](topic, "a", "1"))
    producer.send(new ProducerRecord[String, String](topic, "b", "2"))
    producer.send(new ProducerRecord[String, String](topic, "c", "3"))
    producer.send(new ProducerRecord[String, String](topic, "d", "4"))
    producer.send(new ProducerRecord[String, String](topic, "e", "27-7-22"))
    producer.send(new ProducerRecord(topic, "name", "6"))
    print("sended")
    producer.close()
  }
}
package com.project.demo
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
object Consumer {
  def main(args: Array[String]): Unit = {
    consumeFromKafka("quickstart-events")
  }
  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    var while_iteration=0
    while (true) {
      while_iteration+=1
      
      val record = consumer.poll(1000).asScala
//      println(record)
      var for_iteration=0
      for (data <- record.iterator){
        for_iteration+=1
        println(while_iteration,for_iteration)
        println(data.key(),"key",data.value(),"value")
      }
    }
  }
  
}
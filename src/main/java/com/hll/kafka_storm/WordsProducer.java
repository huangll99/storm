package com.hll.kafka_storm;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by hll on 2016/6/6.
 */
public class WordsProducer {

  static String words = "Now, we will create the WordsProducer class in the com.learningstorm.kafka package.\n" +
      "This class will produce each word from the first paragraph of Franz Kafka's\n" +
      "Metamorphosis into the words_topic topic in Kafka as a single message. The following is\n" +
      "the code of the WordsProducer class with explanation.";

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("metadata.broker.list", "192.168.70.30:9093");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    Producer producer = new Producer(config);

    for (String word : words.split("\\s")) {
      KeyedMessage<String, String> data = new KeyedMessage<>("words-topic", word);
      producer.send(data);
    }

    System.out.println("send end................");
    producer.close();
  }
}

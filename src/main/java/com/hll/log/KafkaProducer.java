package com.hll.log;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.util.Properties;

/**
 * Created by hll on 2016/6/12.
 */
public class KafkaProducer {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("metadata.broker.list", "192.168.70.30:9093");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<>(config);
    try {
      FileInputStream fis = new FileInputStream("./src/main/resources/apache_test.log");
      BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        KeyedMessage<String, String> data = new KeyedMessage<>("apache_log2", line);
        producer.send(data);
      }
      reader.close();
      fis.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    producer.close();
  }
}

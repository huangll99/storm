package com.hll.kafka_storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by hll on 2016/6/7.
 */
public class KafkaTopology {

  public static void main(String[] args) throws InterruptedException {
    ZkHosts zkHosts = new ZkHosts("192.168.70.20:2184");
    SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "words-topic", "", "id");
    kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
    builder.setBolt("SentenceBolt", new SentenceBolt(), 1).globalGrouping("KafkaSpout");
    builder.setBolt("PrinterBolt", new PrinterBolt(), 1).globalGrouping("SentenceBolt");

    LocalCluster cluster = new LocalCluster();
    Config config = new Config();
    cluster.submitTopology("KafkaTopology", config, builder.createTopology());
    System.out.println("waiting to consume from kafka");
    Thread.sleep(10000000);

    cluster.killTopology("KafkaTopology");
    cluster.shutdown();
  }
}

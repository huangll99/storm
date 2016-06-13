package com.hll.log;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by hll on 2016/6/13.
 */
public class LogProcessingTopology {
  public static void main(String[] args) throws InterruptedException {
    ZkHosts zkHosts = new ZkHosts("192.168.70.20:2184");
    SpoutConfig spoutConfig = new SpoutConfig(zkHosts, "apache_log2", "", "id");
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig), 1);
    builder.setBolt("LogSplitter", new ApacheLogSplitterBolt(), 1).globalGrouping("KafkaSpout");
    builder.setBolt("IpToCountry", new UserInformationGetterBolt("E:\\projects\\storm\\src\\main\\resources\\GeoIP.dat"), 1).globalGrouping("LogSplitter");
    builder.setBolt("PersistenceBolt", new PersistenceBolt(), 1).globalGrouping("IpToCountry");


    LocalCluster cluster = new LocalCluster();
    Config config = new Config();
    cluster.submitTopology("LogProcessingTopology", config, builder.createTopology());
    System.out.println("waiting to consume from kafka");
    Thread.sleep(10000000);

    cluster.killTopology("KafkaTopology");
    cluster.shutdown();

  }
}

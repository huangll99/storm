package com.hll.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by hll on 2016/5/25.
 */
public class WordCountTopology {

  private static final String SENTENCE_SPOUT_ID = "sentence-spout";
  private static final String SPLIT_BOLT_ID = "split-bolt";
  private static final String COUNT_BOLT_ID = "count-bolt";
  private static final String REPORT_BOLT_ID = "report-bolt";
  private static final String TOPOLOGY_NAME = "word-count-topology";

  public static void main(String[] args) {
    SentenceSpout spout = new SentenceSpout();
    SplitBolt splitBolt = new SplitBolt();
    WordCountBolt countBolt = new WordCountBolt();
    ReportBolt reportBolt = new ReportBolt();
//    EsConfig esConfig = new EsConfig("es-cluster", new String[]{"192.168.70.30:9200", "192.168.70.20:9200"});
//    EsTupleMapper tupleMapper=new DefaultEsTupleMapper();
//    EsIndexBolt indexBolt = new EsIndexBolt(esConfig,tupleMapper);

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout(SENTENCE_SPOUT_ID, spout);
    builder.setBolt(SPLIT_BOLT_ID, splitBolt,4).shuffleGrouping(SENTENCE_SPOUT_ID);
    builder.setBolt(COUNT_BOLT_ID, countBolt,4).fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));
    builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);


    Config config = new Config();
    //config.setDebug(true);
    config.setNumWorkers(2);
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    Utils.sleep(100000);
    cluster.killTopology(TOPOLOGY_NAME);
    cluster.shutdown();
  }
}

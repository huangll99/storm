package com.hll.storm_es;

import com.ikongjian.storm_es.bolt.EsIndexBolt;
import com.ikongjian.storm_es.common.DefaultEsTupleMapper;
import com.ikongjian.storm_es.common.EsConfig;
import com.ikongjian.storm_es.common.EsTupleMapper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Created by hll on 2016/5/25.
 */
public class StormToEsTopology {

  private static final String SENTENCE_SPOUT_ID = "sentence-spout";
  private static final String REPORT_BOLT_ID = "report-bolt";
  private static final String TOPOLOGY_NAME = "word-count-topology";

  public static void main(String[] args) {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout());
    builder.setBolt(REPORT_BOLT_ID, new ReportBolt()).globalGrouping(SENTENCE_SPOUT_ID);
    EsConfig esConfig = new EsConfig("es-cluster", new String[]{"192.168.70.30:9300"});
    EsTupleMapper tupleMapper = new DefaultEsTupleMapper();
    EsIndexBolt indexBolt = new EsIndexBolt(esConfig, tupleMapper);
    builder.setBolt("es-bolt", indexBolt).shuffleGrouping(REPORT_BOLT_ID);

    Config config = new Config();
    //config.setDebug(true);
    config.setNumWorkers(2);
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    Utils.sleep(200000);
    cluster.killTopology(TOPOLOGY_NAME);
    cluster.shutdown();
  }
}

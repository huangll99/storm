package com.hll.ch1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by hll on 2016/5/31.
 */
public class LearningStormTopology {

  public static void main(String[] args) throws InterruptedException {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("LearningStormSpout", new LearningStormSpout(), 2);
    builder.setBolt("LearningStormBolt", new LearningStormBolt(), 2).shuffleGrouping("LearningStormSpout");
    Config conf = new Config();
    conf.setDebug(false);
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("LearningStormTopology", conf, builder.createTopology());
    Thread.sleep(100000);
    cluster.killTopology("LearningStormTopology");
    cluster.shutdown();
  }

}

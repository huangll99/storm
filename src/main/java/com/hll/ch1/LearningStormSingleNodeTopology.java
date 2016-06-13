package com.hll.ch1;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by hll on 2016/5/31.
 */
public class LearningStormSingleNodeTopology {

  public static void main(String[] args) throws InterruptedException {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("LearningStormSpout", new LearningStormSpout(), 2);
    builder.setBolt("LearningStormBolt", new LearningStormBolt(), 2).shuffleGrouping("LearningStormSpout");
    Config conf = new Config();
    conf.setDebug(false);
    try {
      StormSubmitter.submitTopology("LearningStormSingleNodeTopology", conf, builder.createTopology());
    } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
      e.printStackTrace();
    }
  }

}

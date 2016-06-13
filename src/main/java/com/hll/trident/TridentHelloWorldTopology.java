package com.hll.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

/**
 * Created by hll on 2016/6/7.
 */
public class TridentHelloWorldTopology {
  public static void main(String[] args) {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("Count", conf, buildTopology());
  }

  private static StormTopology buildTopology() {
    FakeTweetSpout spout = new FakeTweetSpout(10);
    TridentTopology topology = new TridentTopology();
    topology.newStream("faketweetspout", spout).shuffle()
        .each(new Fields("text", "Country"), new TridentUtility.TweetFilter())
        .groupBy(new Fields("Country")).aggregate(new Fields("Country"), new Count(), new Fields("count"))
        .each(new Fields("count"), new TridentUtility.Print()).parallelismHint(2);

    return topology.build();
  }
}

package com.hll.ch1;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by hll on 2016/5/31.
 */
public class LearningStormSpout extends BaseRichSpout {

  private static final long serialVersionUID = -8036791654265511511L;

  private SpoutOutputCollector collector;
  private static final Map<Integer, String> map = new HashMap<>();

  static {
    map.put(0, "google");
    map.put(1, "facebook");
    map.put(2, "twitter");
    map.put(3, "youtube");
    map.put(4, "linkedin");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("site"));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    Random rand = new Random();
    int randomNumber = rand.nextInt(5);
    collector.emit(new Values(map.get(randomNumber)));

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

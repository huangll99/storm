package com.hll.ch1;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by hll on 2016/5/31.
 */
public class LearningStormBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 7767321100253120784L;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String site = input.getStringByField("site");
    System.out.println("name of the site is: " + site);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}

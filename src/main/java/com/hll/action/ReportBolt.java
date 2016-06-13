package com.hll.action;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by hll on 2016/5/25.
 */
public class ReportBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 6436373068049605688L;

  private HashMap<String, Long> counts;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    counts = new HashMap<>();
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String word = input.getStringByField("word");
    Long count = input.getLongByField("count");
    this.counts.put(word, count);
  //  System.out.println(word + " -- " + count);
    try {
      Thread.sleep(10000000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}

package com.hll.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.stream.Collector;

/**
 * Created by hll on 2016/5/25.
 */
public class ReportBolt extends BaseRichBolt {

  private static final long serialVersionUID = 6436373068049605688L;

  private HashMap<String, Long> counts;

  private OutputCollector collector;

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.counts = new HashMap<String, Long>();
    this.collector = outputCollector;
  }

  public void execute(Tuple tuple) {
    String word = tuple.getStringByField("word");
    Long count = tuple.getLongByField("count");
    this.counts.put(word, count);
    System.out.println(word + " -- " + count);
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }

}

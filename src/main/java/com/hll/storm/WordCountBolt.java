package com.hll.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hll on 2016/5/25.
 */
public class WordCountBolt extends BaseRichBolt {

  private HashMap<String, Long> counts;

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.counts = new HashMap<>();
  }

  public void execute(Tuple tuple) {
    String word = tuple.getStringByField("word");
    Long count = this.counts.get(word);
    if (count == null) {
      count = 0L;
    }
    count++;
    this.counts.put(word, count);
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }
}

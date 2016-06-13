package com.hll.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by hll on 2016/5/25.
 */
public class SplitBolt extends BaseRichBolt {

  private static final long serialVersionUID = -2068779618487474690L;
  private OutputCollector collector;

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  public void execute(Tuple tuple) {
    String sentence = tuple.getStringByField("sentence");
    String[] words = sentence.split(" ");
    for (String word : words) {
      this.collector.emit(new Values(word));
    }

  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }
}

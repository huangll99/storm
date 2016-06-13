package com.hll.action;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by hll on 2016/5/25.
 */
public class SplitBolt extends BaseBasicBolt {

  private static final long serialVersionUID = -2068779618487474690L;

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String sentence = input.getStringByField("sentence");
    String[] words = sentence.split(" ");
    for (String word : words) {
      collector.emit(new Values(word));
    }
  }
}

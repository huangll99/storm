package com.hll.storm_es;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Created by hll on 2016/5/25.
 */
public class SentenceSpout extends BaseRichSpout {

  private static final long serialVersionUID = -5993403238886342865L;
  private SpoutOutputCollector collector;
  private String[] sentences = {"my dog has fleas", "i like cold beverages",
      "the dog ate my homework", "don't have a cow man",
      "i don't think i like fleas"};
  private int index = 0;

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("sentence"));
  }

  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.collector = spoutOutputCollector;
  }

  public void nextTuple() {
    this.collector.emit(new Values(sentences[index]));
    index++;
    if (index >= sentences.length) {
      index = 0;
    }
    Utils.sleep(100);
  }
}

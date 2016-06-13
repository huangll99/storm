package com.hll.storm_es;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by hll on 2016/5/25.
 */
public class ReportBolt extends BaseRichBolt {

  private static final long serialVersionUID = 6436373068049605688L;

  private OutputCollector collector;

  private long id = 0;

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.collector = outputCollector;
  }

  public void execute(Tuple tuple) {
    MessageId messageId = tuple.getMessageId();
    System.out.println("messageid-->" + messageId);
    String sentence = tuple.getStringByField("sentence");
    collector.emit(new Values("storm", "sentence", ++id + "", "{\"sentence\":\"" + sentence + "\"}"));
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("index", "type", "id", "source"));
  }

}

package com.hll.kafka_storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by hll on 2016/6/6.
 */
public class PrinterBolt extends BaseBasicBolt {
  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String sentence = input.getString(0);
    System.out.println("received sentence:"+sentence);

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}

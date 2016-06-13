package com.hll.log;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by hll on 2016/6/13.
 */
public class PersistenceBolt extends BaseRichBolt {

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
  }

  @Override
  public void execute(Tuple tuple) {
    System.out.println(tuple);
    DBHelper.persist(tuple);
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}

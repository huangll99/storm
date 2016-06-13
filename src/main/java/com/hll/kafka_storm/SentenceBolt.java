package com.hll.kafka_storm;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.org.eclipse.jetty.util.StringUtil;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hll on 2016/6/6.
 */
public class SentenceBolt extends BaseBasicBolt{

  private List<String> words = new ArrayList<>();

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String word = input.getString(0);
    if (StringUtils.isBlank(word)){
      return;
    }
    System.out.println("received word:"+word);
    words.add(word);
    if (word.endsWith(".")){
      String join = StringUtils.join(words, " ");
      System.out.println("join  -- "+join);
      ArrayList<Object> list = new ArrayList<>();
      list.add(join);
      collector.emit(list);
      words.clear();
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }
}

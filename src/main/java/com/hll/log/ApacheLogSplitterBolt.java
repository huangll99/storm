package com.hll.log;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hll on 2016/6/12.
 */
public class ApacheLogSplitterBolt extends BaseBasicBolt {

  private static final long serialVersionUID = -4858729785081836418L;

  private static final ApacheLogSplitter splitter = new ApacheLogSplitter();
  private static final List<String> LOG_ELEMENTS = new ArrayList<>();

  static {
    LOG_ELEMENTS.add("ip");
    LOG_ELEMENTS.add("dateTime");
    LOG_ELEMENTS.add("request");
    LOG_ELEMENTS.add("response");
    LOG_ELEMENTS.add("bytesSent");
    LOG_ELEMENTS.add("referrer");
    LOG_ELEMENTS.add("useragent");
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String log = input.getString(0);
    if (StringUtils.isBlank(log)){
      return;
    }
    Map<String, Object> logMap = splitter.logSplitter(log);
    List<Object> logData = new ArrayList<>();
    for (String element:LOG_ELEMENTS){
      logData.add(logMap.get(element));
    }
    collector.emit(logData);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ip", "dateTime", "request", "response", "bytesSent",
        "referrer", "useragent"));
  }
}

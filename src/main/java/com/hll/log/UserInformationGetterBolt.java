package com.hll.log;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by hll on 2016/6/13.
 */
public class UserInformationGetterBolt extends BaseRichBolt {

  private static final long serialVersionUID = 8786749983183477002L;

  private IpToCountryConverter converter;
  private UserAgent userAgent;

  private OutputCollector collector;
  private String pathToGeoLiteCityFile;

  public UserInformationGetterBolt(String pathToGeoLiteCityFile) {
    this.pathToGeoLiteCityFile = pathToGeoLiteCityFile;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.converter = new IpToCountryConverter(this.pathToGeoLiteCityFile);
  }

  @Override
  public void execute(Tuple input) {
    String ip = input.getStringByField("ip").toString();
    String country = converter.ipToCountry(ip);
    UserAgent useragent = UserAgent.parseUserAgentString(input.getStringByField("useragent"));
    collector.emit(new Values(input.getString(0), input.getString(1), input.getString(2), input.getString(3),
        input.getString(4), input.getString(5), input.getString(6),
        country, useragent.getBrowser().getName(), useragent.getOperatingSystem().getName()));
    System.out.println("emit...");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("ip", "dateTime", "request", "response",
        "bytesSent", "referrer", "useragent", "country", "browser",
        "os"));
  }

  public static void main(String[] args) {
    UserAgent userAgent = UserAgent.parseUserAgentString("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36");
    System.out.println(userAgent.getBrowser());
    System.out.println(userAgent.getOperatingSystem());
  }
}

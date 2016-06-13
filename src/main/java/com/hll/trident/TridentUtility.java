package com.hll.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Created by hll on 2016/6/7.
 */
public class TridentUtility {
  public static class Split extends BaseFunction {

    private static final long serialVersionUID = -6576523307076933877L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String countries = tuple.getString(0);
      for (String word : countries.split(",")) {
        collector.emit(new Values(word));
      }
    }
  }

  public static class TweetFilter extends BaseFilter {
    private static final long serialVersionUID = -2143508480660159201L;

    @Override
    public boolean isKeep(TridentTuple tuple) {
      if (tuple.getString(0).contains("#FIFA")) {
        return true;
      } else {
        return false;
      }
    }
  }

  public static class Print extends BaseFilter {
    private static final long serialVersionUID = -1035604142958781773L;

    @Override
    public boolean isKeep(TridentTuple tuple) {
      System.out.println(tuple);
      return true;
    }
  }
}

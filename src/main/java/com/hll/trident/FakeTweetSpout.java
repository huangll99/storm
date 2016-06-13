package com.hll.trident;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * Created by hll on 2016/6/7.
 */
public class FakeTweetSpout implements IBatchSpout {
  private static final long serialVersionUID = 2551067283595425217L;

  private int batchSize;
  private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<>();

  public FakeTweetSpout(int batchSize) {
    this.batchSize = batchSize;
  }

  private static final Map<Integer, String> TWEET_MAP = new HashMap<>();

  static {
    TWEET_MAP.put(0, " Adidas #FIFA WorldCup Chant Challenge ");
    TWEET_MAP.put(1, "#FIFA worldcup");
    TWEET_MAP.put(2, "#FIFA worldcup");
    TWEET_MAP.put(3, " The Great Gatsby is such a good #movie ");
    TWEET_MAP.put(4, "#Movie top 10");
  }

  private static final Map<Integer, String> COUNTRY_MAP = new HashMap<>();

  static {
    COUNTRY_MAP.put(0, "United State");
    COUNTRY_MAP.put(1, "Japan");
    COUNTRY_MAP.put(2, "India");
    COUNTRY_MAP.put(3, "China");
    COUNTRY_MAP.put(4, "Brazil");
  }

  private List<Object> recordGenerator() {
    final Random rand = new Random();
    int rand1 = rand.nextInt(5);
    int rand2 = rand.nextInt(5);
    return new Values(TWEET_MAP.get(rand1), COUNTRY_MAP.get(rand2));
  }

  @Override
  public void open(Map conf, TopologyContext context) {

  }

  @Override
  public void emitBatch(long batchId, TridentCollector collector) {
    System.out.println("batchid ---------- " + batchId);

    List<List<Object>> batches = this.batchesMap.get(batchId);
    if (batches == null) {
      batches = new ArrayList<>();
      for (int i = 0; i < this.batchSize; i++) {
        batches.add(this.recordGenerator());
      }
      this.batchesMap.put(batchId, batches);
    }
    for (List<Object> list : batches) {
      collector.emit(list);
    }
  }

  @Override
  public void ack(long batchId) {
    this.batchesMap.remove(batchId);
  }

  @Override
  public void close() {

  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("text", "Country");
  }
}

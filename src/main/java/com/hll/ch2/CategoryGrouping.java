package com.hll.ch2;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by hll on 2016/6/6.
 */
public class CategoryGrouping implements CustomStreamGrouping, Serializable {
  private static final long serialVersionUID = -8150164930081116995L;

  private static final Map<String, Integer> categories = ImmutableMap.of(
      "Financial", 0,
      "Medical", 1,
      "FMCG", 2,
      "Electronics", 3
  );

  private int tasks;

  @Override
  public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
    this.tasks = targetTasks.size();
  }

  @Override
  public List<Integer> chooseTasks(int taskId, List<Object> values) {
    String category = (String) values.get(0);
    return ImmutableList.of(categories.get(category) % tasks);
  }
}

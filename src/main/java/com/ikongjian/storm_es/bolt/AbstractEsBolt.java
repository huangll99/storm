/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ikongjian.storm_es.bolt;

import com.ikongjian.storm_es.common.EsConfig;
import com.ikongjian.storm_es.common.StormElasticSearchClient;
import org.apache.storm.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.storm.shade.com.google.common.base.Preconditions.checkNotNull;


public abstract class AbstractEsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(com.ikongjian.storm_es.bolt.AbstractEsBolt.class);

    protected static Client client;

    protected OutputCollector collector;
    private EsConfig esConfig;

    public AbstractEsBolt(EsConfig esConfig) {
        checkNotNull(esConfig);
        this.esConfig = esConfig;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.collector = outputCollector;
            synchronized (com.ikongjian.storm_es.bolt.AbstractEsBolt.class) {
                if (client == null) {
                    client = new StormElasticSearchClient(esConfig).construct();
                }
            }
        } catch (Exception e) {
            LOG.warn("unable to initialize EsBolt ", e);
        }
    }

    @Override
    public abstract void execute(Tuple tuple);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @VisibleForTesting
    static Client getClient() {
        return client;
    }

    @VisibleForTesting
    static void replaceClient(Client client) {
        com.ikongjian.storm_es.bolt.AbstractEsBolt.client = client;
    }
}

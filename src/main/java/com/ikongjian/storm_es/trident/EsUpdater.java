/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ikongjian.storm_es.trident;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

public class EsUpdater extends BaseStateUpdater<com.ikongjian.storm_es.trident.EsState> {
    /**
     * {@inheritDoc}
     * Each tuple should have relevant fields (source, index, type, id) for EsState's tupleMapper to extract ES document.
     */
    @Override
    public void updateState(com.ikongjian.storm_es.trident.EsState state, List<TridentTuple> tuples, TridentCollector collector) {
        state.updateState(tuples);
    }
}

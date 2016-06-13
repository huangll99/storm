/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ikongjian.storm_es.common;

import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

final class TransportAddresses implements Iterable<InetSocketTransportAddress> {

  static final String DELIMETER = ":";

  private final String[] nodes;

  TransportAddresses(String[] nodes) {
    if (nodes == null) {
      throw new IllegalArgumentException("Elasticsearch hosts cannot be null");
    }
    if (nodes.length == 0) {
      throw new IllegalArgumentException("At least one Elasticsearch host must be specified");
    }

    this.nodes = nodes;
  }

  @Override
  public Iterator<InetSocketTransportAddress> iterator() {
    List<InetSocketTransportAddress> result = new LinkedList<>();

    for (String node : nodes) {
      InetSocketTransportAddress transportAddress = transformToInetAddress(node);
      result.add(transportAddress);
    }

    return result.iterator();
  }

  private InetSocketTransportAddress transformToInetAddress(String node) {
    String[] hostAndPort = node.split(DELIMETER);
    if (hostAndPort.length != 2) {
      throw new IllegalArgumentException(
          "Incorrect Elasticsearch node format, should follow {host}" + DELIMETER + "{port} pattern");
    }
    String hostname = hostname(hostAndPort[0]);
    try {
      return new InetSocketTransportAddress(InetAddress.getByName(hostname), port(hostAndPort[1]));
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private String hostname(String input) {
    return input.trim();
  }

  private int port(String input) {
    return Integer.parseInt(input.trim());
  }
}

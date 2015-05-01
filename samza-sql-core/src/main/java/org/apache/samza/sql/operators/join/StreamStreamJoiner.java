/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.operators.join;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.sql.operators.window.FullStateTimeWindow;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.Range;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;


/**
 * This class implements a simple stream-to-stream join
 */
public class StreamStreamJoiner extends SimpleOperator implements TupleOperator {
  private final StreamStreamJoinSpec spec;

  private Map<EntityName, FullStateTimeWindow> inputWindows = new HashMap<EntityName, FullStateTimeWindow>();

  public StreamStreamJoiner(StreamStreamJoinSpec spec) {
    super(spec);
    this.spec = spec;
  }

  //TODO: stub constructor to allow compilation pass. Need to construct real StreamStreamJoinSpec.
  public StreamStreamJoiner(String opId, List<String> inputRelations, String output, List<String> joinKeys) {
    super(null);
    this.spec = null;
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
    // initialize the inputWindows map

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void process(Tuple tuple, SqlMessageCollector collector) throws Exception {
    // TODO Auto-generated method stub
    Map<EntityName, Stream> joinSets = findJoinSets(tuple);
    join(tuple, joinSets);
  }

  private void join(Tuple tuple, Map<EntityName, Stream> joinSets) {
    // TODO Auto-generated method stub
    // Do M-way joins if necessary, it should be ordered based on the orders of the input relations in inputs
    // NOTE: inner joins may be optimized by re-order the input relations by joining inputs w/ less join sets first. We will consider it later.

  }

  private Map<EntityName, Stream> findJoinSets(Tuple tuple) {
    // TODO Auto-generated method stub
    Map<EntityName, Stream> streamSets = new HashMap<EntityName, Stream>();
    for (EntityName strmName : spec.getInputNames()) {
      if (strmName.equals(tuple.getEntityName())) {
        continue;
      }
      KeyValueIterator<OrderedStoreKey, Tuple> tuples = getJoinSet(tuple, strmName);
      streamSets.put(strmName, new Stream<OrderedStoreKey>() {
        @SuppressWarnings("serial")
        private Map<OrderedStoreKey, Tuple> tuplesMap = new LinkedHashMap<OrderedStoreKey, Tuple>() {
          {
            for (; tuples.hasNext();) {
              Entry<OrderedStoreKey, Tuple> entry = tuples.next();
              put(entry.getKey(), entry.getValue());
            }
          }
        };

        @Override
        public EntityName getName() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public Tuple get(OrderedStoreKey key) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public void put(OrderedStoreKey key, Tuple value) {
          // TODO Auto-generated method stub

        }

        @Override
        public void putAll(List<Entry<OrderedStoreKey, Tuple>> entries) {
          // TODO Auto-generated method stub

        }

        @Override
        public void delete(OrderedStoreKey key) {
          // TODO Auto-generated method stub

        }

        @Override
        public KeyValueIterator<OrderedStoreKey, Tuple> range(OrderedStoreKey from, OrderedStoreKey to) {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public KeyValueIterator<OrderedStoreKey, Tuple> all() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public void close() {
          // TODO Auto-generated method stub

        }

        @Override
        public void flush() {
          // TODO Auto-generated method stub

        }

        @Override
        public List<String> getOrderFields() {
          // TODO Auto-generated method stub
          return null;
        }

      });
    }

    return streamSets;
  }

  private KeyValueIterator<OrderedStoreKey, Tuple> getJoinSet(Tuple tuple, EntityName strmName) {
    FullStateTimeWindow wndOp = inputWindows.get(strmName);
    // default to find time range
    Range<Long> timeRange = getTimeRange(tuple, strmName);
    List<Entry<String, Object>> equalFieldValues = getEqualFields(tuple, strmName);
    return wndOp.getMessages(timeRange, equalFieldValues);
  }

  private List<Entry<String, Object>> getEqualFields(Tuple tuple, EntityName strmName) {
    // TODO Auto-generated method stub
    return null;
  }

  private Range<Long> getTimeRange(Tuple tuple, EntityName strmName) {
    // TODO Auto-generated method stub
    // This is to deduce the time range to query from the input tuple and the join condition on timestamp field
    // The join condition should be something that can calculate the join key range based on the input tuple and the
    return null;
  }
}

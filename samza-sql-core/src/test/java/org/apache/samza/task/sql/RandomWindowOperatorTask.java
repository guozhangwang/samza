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

package org.apache.samza.task.sql;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.factory.DefaultOperatorCallback;
import org.apache.samza.sql.operators.window.FullStateTimeWindowOp;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.WindowOutputStream;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a use case for the full-state timed window operator
 *
 */
public class RandomWindowOperatorTask implements StreamTask, InitableTask, WindowableTask {
  private FullStateTimeWindowOp wndOp;

  private class MyOperatorCallback extends DefaultOperatorCallback {
    @Override
    public Tuple beforeSend(Tuple tuple, MessageCollector collector, TaskCoordinator coordinator) {
      collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "joinOutput1"), tuple.getKey(), tuple
          .getMessage()));
      // message has been intercepted and sent, return null to stop
      return null;
    }

    @Override
    public Relation beforeSend(Relation rel, MessageCollector collector, TaskCoordinator coordinator) {
      processWindowOutput((WindowOutputStream<OrderedStoreKey>) rel, collector);
      // all results are processed, return null to stop
      return null;
    }

    private void processWindowOutput(WindowOutputStream<OrderedStoreKey> wndOut, MessageCollector collector) {
      // get each tuple in the join operator's outputs and send it to system stream
      KeyValueIterator<OrderedStoreKey, Tuple> iter = wndOut.all();
      while (iter.hasNext()) {
        Tuple otuple = iter.next().getValue();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "joinOutput1"), otuple.getKey(), otuple
            .getMessage()));
      }
    }
  }

  private OperatorCallback wndOpCallback = new MyOperatorCallback();

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // based on tuple's stream name, get the window op and run process()
    wndOp.process(new IncomingMessageTuple(envelope), collector, coordinator);

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // based on tuple's stream name, get the window op and run process()
    wndOp.refresh(System.nanoTime(), collector, coordinator);
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // 1. create a fixed length 10 sec window operator
    this.wndOp = new FullStateTimeWindowOp("wndOp1", 10, "kafka:stream1", "relation1", wndOpCallback);
    this.wndOp.init(config, context);
  }
}

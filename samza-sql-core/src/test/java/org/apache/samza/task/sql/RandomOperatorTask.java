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

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.join.StreamStreamJoin;
import org.apache.samza.sql.operators.window.FullStateTimeWindowOp;
import org.apache.samza.sql.window.storage.OrderedStoreKey;
import org.apache.samza.sql.window.storage.WindowOutputStream;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
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
 * This example illustrate a SQL join operation that joins two streams together using the following operations:
 * <p>a. the two streams are each processed by a window operator to convert to relations
 * <p>b. a join operator is applied on the two relations to generate join results
 * <p>c. finally, the join results are sent out to the system output
 *
 */
public class RandomOperatorTask implements StreamTask, InitableTask, WindowableTask {
  private KeyValueStore<EntityName, List<Object>> opOutputStore;
  private FullStateTimeWindowOp wndOp1;
  private FullStateTimeWindowOp wndOp2;
  private StreamStreamJoin joinOp;

  private FullStateTimeWindowOp getWindowOp(EntityName streamName) {
    if (streamName.equals(EntityName.getStreamName("kafka:stream1"))) {
      return this.wndOp1;
    } else if (streamName.equals(EntityName.getStreamName("kafka:stream2"))) {
      return this.wndOp2;
    }

    throw new IllegalArgumentException("No window operator found for stream: " + streamName);
  }

  private void processJoinOutput(List<Object> outputs, MessageCollector collector) {
    // get each tuple in the join operator's outputs and send it to system stream
    for (Object joinOutput : outputs) {
      for (KeyValueIterator<Object, Tuple> iter = ((Relation) joinOutput).all(); iter.hasNext();) {
        Tuple otuple = iter.next().getValue();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "joinOutput1"), otuple.getKey(), otuple
            .getMessage()));
      }
    }
  }

  private void joinWindowOutput(WindowOutputStream<OrderedStoreKey> output, SqlMessageCollector sqlCollector)
      throws Exception {
    // TODO: update the interface of the StreamStreamJoinOp s.t. it can be more intuitive to human
    // process all output from the window operator
    KeyValueIterator<OrderedStoreKey, Tuple> iter = output.all();
    while (iter.hasNext()) {
      this.joinOp.process(iter.next().getValue(), sqlCollector);
    }
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // create the StoreMessageCollector
    StoreMessageCollector sqlCollector = new StoreMessageCollector(this.opOutputStore);

    // construct the input tuple
    IncomingMessageTuple ituple = new IncomingMessageTuple(envelope);

    // based on tuple's stream name, get the window op and run process()
    FullStateTimeWindowOp wndOp = getWindowOp(ituple.getEntityName());
    wndOp.addMessage(ituple);
    WindowOutputStream<OrderedStoreKey> output = wndOp.getResult();
    if (output == null) {
      return;
    }

    joinWindowOutput(output, sqlCollector);
    // get the output from the join operator and send them
    processJoinOutput(sqlCollector.removeOutput(this.joinOp.getSpec().getOutputNames().get(0)), collector);

    // flush the window operator states and clear up the output, since the output has been processed successfully.
    wndOp.flush();

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // create the StoreMessageCollector
    StoreMessageCollector sqlCollector = new StoreMessageCollector(this.opOutputStore);

    // based on tuple's stream name, get the window op and run process()
    wndOp1.refresh();
    wndOp2.refresh();

    // TODO: update the interface of the StreamStreamJoinOp s.t. it can be more intuitive to human
    // process all output from the window operator
    WindowOutputStream<OrderedStoreKey> wndOut1 = wndOp1.getResult();
    WindowOutputStream<OrderedStoreKey> wndOut2 = wndOp2.getResult();

    if (wndOut1 != null) {
      joinWindowOutput(wndOut1, sqlCollector);
    }

    if (wndOut2 != null) {
      joinWindowOutput(wndOut2, sqlCollector);
    }

    // get the output from the join operator and send them
    processJoinOutput(sqlCollector.removeOutput(this.joinOp.getSpec().getOutputNames().get(0)), collector);

    // flush the window operator states and clear up the output, since the output has been processed successfully.
    wndOp1.flush();
    wndOp2.flush();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // 1. create a fixed length 10 sec window operator
    this.wndOp1 = new FullStateTimeWindowOp("wndOp1", 10, "kafka:stream1", "relation1");
    this.wndOp2 = new FullStateTimeWindowOp("wndOp2", 10, "kafka:stream2", "relation2");
    // 2. create a join operation
    List<String> inputRelations = new ArrayList<String>();
    inputRelations.add("relation1");
    inputRelations.add("relation2");
    List<String> joinKeys = new ArrayList<String>();
    joinKeys.add("key1");
    joinKeys.add("key2");
    this.joinOp = new StreamStreamJoin("joinOp", inputRelations, "joinOutput", joinKeys);
    // Finally, initialize all operators
    this.opOutputStore =
        (KeyValueStore<EntityName, List<Object>>) context.getStore("samza-sql-operator-output-kvstore");
    this.wndOp1.init(config, context, null);
    this.wndOp2.init(config, context, null);
    this.joinOp.init(config, context, null);
  }

}

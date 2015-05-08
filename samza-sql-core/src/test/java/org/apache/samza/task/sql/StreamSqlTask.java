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
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.factory.SimpleOperatorFactoryImpl;
import org.apache.samza.sql.operators.factory.SimpleRouter;
import org.apache.samza.sql.operators.join.StreamStreamJoin;
import org.apache.samza.sql.operators.join.StreamStreamJoinSpec;
import org.apache.samza.sql.operators.partition.PartitionOp;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.window.FullStateTimeWindowOp;
import org.apache.samza.sql.operators.window.WindowOpSpec;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a SQL join operation that joins two streams together using the folowing operations:
 * <ul>
 * <li>a. the two streams are each processed by a window operator to convert to relations
 * <li>b. a join operator is applied on the two relations to generate join results
 * <li>c. an istream operator is applied on join output and convert the relation into a stream
 * <li>d. a partition operator that re-partitions the output stream from istream and send the stream to system output
 * </ul>
 *
 * This example also uses an implementation of <code>SqlMessageCollector</code> (@see <code>OperatorMessageCollector</code>)
 * that uses <code>OperatorRouter</code> to automatically execute the whole paths that connects operators together.
 */
public class StreamSqlTask implements StreamTask, InitableTask, WindowableTask {

  private SimpleRouter rteCntx;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    this.rteCntx.process(new IncomingMessageTuple(envelope), collector, coordinator);
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    this.rteCntx.refresh(System.nanoTime(), collector, coordinator);
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // create specification of all operators first
    // 1. create 2 window specifications that define 2 windows of fixed length of 10 seconds
    final WindowOpSpec spec1 =
        new WindowOpSpec("fixedWnd1", EntityName.getStreamName("inputStream1"),
            EntityName.getRelationName("fixedWndOutput1"), 10);
    final WindowOpSpec spec2 =
        new WindowOpSpec("fixedWnd2", EntityName.getStreamName("inputStream2"),
            EntityName.getRelationName("fixedWndOutput2"), 10);
    // 2. create a join specification that join the output from 2 window operators together
    @SuppressWarnings("serial")
    List<EntityName> inputRelations = new ArrayList<EntityName>() {
      {
        add(spec1.getOutputName());
        add(spec2.getOutputName());
      }
    };
    @SuppressWarnings("serial")
    List<String> joinKeys = new ArrayList<String>() {
      {
        add("key1");
        add("key2");
      }
    };
    StreamStreamJoinSpec joinSpec =
        new StreamStreamJoinSpec("joinOp", inputRelations, EntityName.getRelationName("joinOutput"), joinKeys);
    // 4. create the specification of a partition operator that re-partitions the stream based on <code>joinKey</code>
    PartitionSpec parSpec =
        new PartitionSpec("parOp1", joinSpec.getOutputName().getName(), new SystemStream("kafka", "parOutputStrm1"),
            "joinKey", 50);

    // create all operators via the operator factory
    // 1. create two window operators
    SimpleOperatorFactoryImpl operatorFactory = new SimpleOperatorFactoryImpl();
    FullStateTimeWindowOp wnd1 = (FullStateTimeWindowOp) operatorFactory.getOperator(spec1);
    FullStateTimeWindowOp wnd2 = (FullStateTimeWindowOp) operatorFactory.getOperator(spec2);
    // 2. create one join operator
    StreamStreamJoin join = (StreamStreamJoin) operatorFactory.getOperator(joinSpec);
    // 3. create one stream operator
    // 4. create a re-partition operator
    PartitionOp par = (PartitionOp) operatorFactory.getOperator(parSpec);

    // Now, connecting the operators via the OperatorRouter
    this.rteCntx = new SimpleRouter();
    // 1. set two system input operators (i.e. two window operators)
    this.rteCntx.addOperator(wnd1);
    this.rteCntx.addOperator(wnd2);
    // 2. connect join operator to both window operators
    this.rteCntx.addOperator(join);
    // 3. connect stream operator to the join operator
    // 4. connect re-partition operator to the stream operator
    this.rteCntx.addOperator(par);

    this.rteCntx.init(config, context);
  }
}

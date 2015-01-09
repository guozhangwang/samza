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

package org.apache.samza.sql.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.IncomingMessageTuple;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.data.SystemInputTuple;
import org.apache.samza.sql.operators.factory.SimpleOperatorFactoryImpl;
import org.apache.samza.sql.operators.partition.PartitionOp;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.relation.Join;
import org.apache.samza.sql.operators.relation.JoinSpec;
import org.apache.samza.sql.operators.routing.SimpleRoutingContext;
import org.apache.samza.sql.operators.stream.InsertStream;
import org.apache.samza.sql.operators.stream.InsertStreamSpec;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.sql.operators.window.WindowSpec;
import org.apache.samza.sql.store.SqlContextManager;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


public class StreamSqlTask implements StreamTask, InitableTask, WindowableTask {

  private SqlContextManager initCntx;

  private OperatorRoutingContext rteCntx;

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub
    RuntimeSystemContext opCntx = new RoutableRuntimeContext(collector, coordinator, this.rteCntx);

    IncomingMessageTuple ituple = new SystemInputTuple(envelope);
    for (Iterator<TupleOperator> iter = this.rteCntx.getSystemInputOps().iterator(ituple.getStreamName()); iter
        .hasNext();) {
      iter.next().process(ituple, opCntx);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
    RuntimeSystemContext opCntx = new RoutableRuntimeContext(collector, coordinator, this.rteCntx);

    long currNano = System.nanoTime();
    for (Iterator<Operator> iter = this.rteCntx.getSystemInputOps().values().iterator(); iter.hasNext();) {
      iter.next().timeout(currNano, opCntx);
    }

  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // create specification of all operators first
    // 1. create 2 window specifications that define 2 windows of fixed length of 10 seconds
    WindowSpec spec1 = new WindowSpec("fixedWnd1", 10, "inputStream1", "fixedWndOutput1");
    WindowSpec spec2 = new WindowSpec("fixedWnd2", 10, "inputStream2", "fixedWndOutput2");
    // 2. create a join specification that join the output from 2 window operators together
    List<String> inputRelations = new ArrayList<String>();
    inputRelations.add(spec1.getOutputName());
    inputRelations.add(spec2.getOutputName());
    List<String> joinKeys = new ArrayList<String>();
    joinKeys.add("key1");
    joinKeys.add("key2");
    JoinSpec joinSpec = new JoinSpec("joinOp", inputRelations, "joinOutput", joinKeys);
    // 3. create the specification of an istream operator that convert the output from join to a stream
    InsertStreamSpec istrmSpec = new InsertStreamSpec("istremOp", joinSpec.getOutputName(), "istrmOutput1");
    // 4. create the specification of a partition operator that re-partitions the stream based on <code>joinKey</code>
    PartitionSpec parSpec =
        new PartitionSpec("parOp1", istrmSpec.getOutputName(), new SystemStream("kafka", "parOutputStrm1"), "joinKey",
            50);

    // create all operators via the operator factory
    // 1. create two window operators
    SimpleOperatorFactoryImpl operatorFactory = new SimpleOperatorFactoryImpl();
    BoundedTimeWindow wnd1 = (BoundedTimeWindow) operatorFactory.getTupleOperator(spec1);
    BoundedTimeWindow wnd2 = (BoundedTimeWindow) operatorFactory.getTupleOperator(spec2);
    // 2. create one join operator
    Join join = (Join) operatorFactory.getRelationOperator(joinSpec);
    // 3. create one stream operator
    InsertStream istream = (InsertStream) operatorFactory.getRelationOperator(istrmSpec);
    // 4. create a re-partition operator
    PartitionOp par = (PartitionOp) operatorFactory.getTupleOperator(parSpec);

    // Now, connecting the operators via the routing context
    this.rteCntx = new SimpleRoutingContext();
    // 1. set two system input operators (i.e. two window operators)
    this.rteCntx.setSystemInputOperator(wnd1);
    this.rteCntx.setSystemInputOperator(wnd2);
    // 2. connect join operator to both window operators
    this.rteCntx.setNextRelationOperator(wnd1.getId(), join);
    this.rteCntx.setNextRelationOperator(wnd2.getId(), join);
    // 3. connect stream operator to the join operator
    this.rteCntx.setNextRelationOperator(join.getId(), istream);
    // 4. connect re-partition operator to the stream operator
    this.rteCntx.setNextTupleOperator(istream.getId(), par);

    // Finally, initialize all operators
    this.initCntx = new SqlContextManager(context);
    for (Iterator<Operator> iter = this.rteCntx.iterator(); iter.hasNext();) {
      iter.next().init(this.initCntx);
    }
  }
}

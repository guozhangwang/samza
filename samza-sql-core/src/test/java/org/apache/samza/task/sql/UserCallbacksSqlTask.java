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
import java.util.Iterator;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.router.OperatorRouter;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.join.StreamStreamJoin;
import org.apache.samza.sql.operators.partition.PartitionOp;
import org.apache.samza.sql.operators.window.FullStateTimeWindowOp;
import org.apache.samza.sql.router.SimpleRouter;
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
public class UserCallbacksSqlTask implements StreamTask, InitableTask, WindowableTask {

  private OperatorRouter rteCntx;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    SqlMessageCollector opCollector = new OperatorMessageCollector(collector, coordinator, this.rteCntx);

    IncomingMessageTuple ituple = new IncomingMessageTuple(envelope);
    for (Iterator<Operator> iter = this.rteCntx.getNextOperators(ituple.getEntityName()).iterator(); iter
        .hasNext();) {
      iter.next().process(ituple, opCollector);
    }

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    SqlMessageCollector opCollector = new OperatorMessageCollector(collector, coordinator, this.rteCntx);
    long nanoSec = System.nanoTime();

    for (EntityName entity : this.rteCntx.getInputEntities()) {
      for (Iterator<Operator> iter = this.rteCntx.getNextOperators(entity).iterator(); iter.hasNext();) {
        iter.next().refresh(nanoSec, opCollector, coordinator);
      }
    }

  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // create all operators via the operator factory
    // 1. create two window operators
    FullStateTimeWindowOp wnd1 = new FullStateTimeWindowOp("fixedWnd1", 10, "inputStream1", "fixedWndOutput1");
    FullStateTimeWindowOp wnd2 = new FullStateTimeWindowOp("fixedWnd2", 10, "inputStream2", "fixedWndOutput2");
    // 2. create one join operator
    @SuppressWarnings("serial")
    List<String> inputRelations = new ArrayList<String>() {
      {
        add("fixedWndOutput1");
        add("fixedWndOutput2");
      }
    };
    @SuppressWarnings("serial")
    List<String> joinKeys = new ArrayList<String>() {
      {
        add("key1");
        add("key2");
      }
    };
    StreamStreamJoin join = new StreamStreamJoin("joinOp", inputRelations, "joinOutput", joinKeys);
    // 4. create a re-partition operator
    PartitionOp par = new PartitionOp("parOp1", "joinOutput", new SystemStream("kafka", "parOutputStrm1"),
        "joinKey", 50);

    // Now, connecting the operators via the OperatorRouter
    this.rteCntx = new SimpleRouter();
    // 1. set two system input operators (i.e. two window operators)
    this.rteCntx.addOperator(wnd1.getSpec().getInputName(), wnd1);
    this.rteCntx.addOperator(wnd2.getSpec().getInputName(), wnd2);
    // 2. connect join operator to both window operators
    this.rteCntx.addOperator(wnd1.getSpec().getOutputName(), join);
    this.rteCntx.addOperator(wnd2.getSpec().getOutputName(), join);
    // 4. connect re-partition operator to the stream operator
    this.rteCntx.addOperator(par.getSpec().getInputNames().get(0), par);

    for (Iterator<Operator> iter = this.rteCntx.iterator(); iter.hasNext();) {
      iter.next().init(config, context, null);
    }
  }
}

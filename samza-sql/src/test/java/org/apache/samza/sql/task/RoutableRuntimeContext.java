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

import org.apache.samza.sql.api.data.OutgoingMessageTuple;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.task.MessageCollector;


/**
 * Example implementation of a runtime context that uses <code>OperatorRoutingContext</code>
 *
 */
public class RoutableRuntimeContext implements RuntimeSystemContext {

  private final MessageCollector collector;
  private final OperatorRoutingContext rteCntx;

  public RoutableRuntimeContext(MessageCollector collector, OperatorRoutingContext rteCntx) {
    this.collector = collector;
    this.rteCntx = rteCntx;
  }

  @Override
  public void send(String currentOpId, Relation deltaRelation) throws Exception {
    this.rteCntx.getNextRelationOperator(currentOpId).process(deltaRelation, this);
  }

  @Override
  public void send(String currentOpId, Tuple tuple) throws Exception {
    if (this.rteCntx.getNextTupleOperator(currentOpId) != null) {
      // by default, always send to the next operator
      this.rteCntx.getNextTupleOperator(currentOpId).process(tuple, this);
    } else if (tuple instanceof OutgoingMessageTuple) {
      // if there is no next operator, check whether the tuple is an OutgoingMessageTuple
      this.collector.send(((OutgoingMessageTuple) tuple).getOutgoingMessageEnvelope());
    }
    throw new IllegalStateException("No next tuple operator found and the tuple is not an OutgoingMessageTuple");
  }

  @Override
  public void send(String currentOpId, long currentSystemNano) throws Exception {
    this.rteCntx.getNextTimeoutOperator(currentOpId).timeout(currentSystemNano, this);
  }

}

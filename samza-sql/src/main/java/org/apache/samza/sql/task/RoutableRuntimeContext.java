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

import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * Example implementation of a runtime context that uses <code>OperatorRoutingContext</code>
 *
 */
public class RoutableRuntimeContext implements RuntimeSystemContext {

  private final MessageCollector collector;
  private final TaskCoordinator coordinator;
  private final OperatorRoutingContext rteCntx;

  public RoutableRuntimeContext(MessageCollector collector, TaskCoordinator coordinator, OperatorRoutingContext rteCntx) {
    this.collector = collector;
    this.coordinator = coordinator;
    this.rteCntx = rteCntx;
  }

  @Override
  public void send(Relation deltaRelation) throws Exception {
    for (RelationOperator op : this.rteCntx.getRelationOperators(deltaRelation.getName())) {
      op.process(deltaRelation, this);
    }
  }

  @Override
  public void send(Tuple tuple) throws Exception {
    for (TupleOperator op : this.rteCntx.getTupleOperators(tuple.getStreamName())) {
      op.process(tuple, this);
    }
  }

  @Override
  public void timeout(List<EntityName> outputs) throws Exception {
    for (EntityName output : outputs) {
      for (Operator op : this.rteCntx.getNextOperators(output)) {
        op.window(this, this.coordinator);
      }
    }
  }

  @Override
  public void send(OutgoingMessageEnvelope envelope) {
    this.collector.send(envelope);
  }

}

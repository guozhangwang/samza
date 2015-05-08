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

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.OperatorCallback;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public class SimpleMessageCollector implements MessageCollector {
  protected final MessageCollector collector;
  protected final TaskCoordinator coordinator;

  public SimpleMessageCollector(MessageCollector collector, TaskCoordinator coordinator) {
    this.collector = collector;
    this.coordinator = coordinator;
  }

  @Override
  public void send(OutgoingMessageEnvelope envelope) {
    this.collector.send(envelope);
  }

  public void send(Relation deltaRelation, OperatorCallback opCallback) throws Exception {
    Relation rel = opCallback.beforeSend(deltaRelation, collector, coordinator);
    if (rel == null) {
      return;
    }
    for (KeyValueIterator<?, Tuple> iter = rel.all(); iter.hasNext();) {
      Entry<?, Tuple> entry = iter.next();
      this.collector.send((OutgoingMessageEnvelope) entry.getValue().getMessage());
    }
  }

  public void send(Tuple tuple, OperatorCallback opCallback) throws Exception {
    Tuple otuple = opCallback.beforeSend(tuple, collector, coordinator);
    if (otuple == null) {
      return;
    }
    this.collector.send((OutgoingMessageEnvelope) otuple.getMessage());
  }

}

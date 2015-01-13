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
import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * Example implementation of runtime context that stores outputs from the operators
 *
 */
public class StoredRuntimeContext implements RuntimeSystemContext {

  private final MessageCollector collector;
  private final TaskCoordinator coordinator;
  private final KeyValueStore<String, List<Object>> outputStore;

  public StoredRuntimeContext(MessageCollector collector, TaskCoordinator coordinator,
      KeyValueStore<String, List<Object>> store) {
    this.collector = collector;
    this.coordinator = coordinator;
    this.outputStore = store;
  }

  @Override
  public void sendToNextRelationOperator(String currentOpId, Relation deltaRelation) throws Exception {
    saveOutput(currentOpId, deltaRelation);
  }

  @Override
  public void sendToNextTupleOperator(String currentOpId, Tuple tuple) throws Exception {
    saveOutput(currentOpId, tuple);
  }

  @Override
  public void sendToNextTimeoutOperator(String currentOpId, long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
  }

  public List<Object> removeOutput(String id) {
    List<Object> output = outputStore.get(id);
    outputStore.delete(id);
    return output;
  }

  private void saveOutput(String currentOpId, Object output) {
    if (this.outputStore.get(currentOpId) == null) {
      this.outputStore.put(currentOpId, new ArrayList<Object>());
    }
    List<Object> outputs = this.outputStore.get(currentOpId);
    outputs.add(output);
  }

}

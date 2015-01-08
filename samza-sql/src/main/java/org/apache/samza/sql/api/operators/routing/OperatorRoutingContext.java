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

package org.apache.samza.sql.api.operators.routing;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.task.RuntimeSystemContext;


public interface OperatorRoutingContext {
  public void setSystemInputOperator(TupleOperator inputOp) throws Exception;

  public void setSystemInputOperator(RelationOperator inputOp) throws Exception;

  public MultiValueMap getSystemInputOps();

  public void setRuntimeContext(RuntimeSystemContext context) throws Exception;

  public RuntimeSystemContext getRuntimeContext();

  public void setNextRelationOperator(String currentOpId, RelationOperator nextOp) throws Exception;

  public void setNextTupleOperator(String currentOpId, TupleOperator nextOp) throws Exception;

  public RelationOperator getNextRelationOperator(String currentOpId);

  public TupleOperator getNextTupleOperator(String currentOpId);

  public Operator getNextTimeoutOperator(String currentOpId);

  public default void sendToNextRelationOperator(String currentOpId, Relation deltaRelation) throws Exception {
    this.getNextRelationOperator(currentOpId).process(deltaRelation, this);
  };

  public default void sendToNextTupleOperator(String currentOpId, Tuple tuple) throws Exception {
    this.getNextTupleOperator(currentOpId).process(tuple, this);
  };

  public default void sendToNextTimeoutOperator(String currentOpId, long currentSystemNano) throws Exception {
    this.getNextTimeoutOperator(currentOpId).timeout(currentSystemNano, this);
  }

}

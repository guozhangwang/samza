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

package org.apache.samza.sql.operators.routing;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;


public class SimpleRoutingContext implements OperatorRoutingContext {
  private Map<String, Operator> operators = new HashMap<String, Operator>();
  private Map<String, Operator> nextOps = new HashMap<String, Operator>();
  private MultiValueMap sysInputOps = new MultiValueMap();

  private void addOperator(Operator op) {
    operators.put(op.getId(), op);
  }

  @Override
  public void setSystemInputOperator(TupleOperator inputOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(inputOp);
    // add the input operator to all input stream spec
    for (String spec : inputOp.getSpec().getInputNames()) {
      sysInputOps.put(spec, inputOp);
    }
  }

  @Override
  public void setSystemInputOperator(RelationOperator inputOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(inputOp);
    // add the input operator to all input relation spec
    for (String spec : inputOp.getSpec().getInputNames()) {
      sysInputOps.put(spec, inputOp);
    }
  }

  @Override
  public MultiValueMap getSystemInputOps() {
    // TODO Auto-generated method stub
    return this.sysInputOps;
  }

  @Override
  public void setNextRelationOperator(String currentOpId, RelationOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(nextOp);
    nextOps.put(currentOpId, nextOp);
  }

  @Override
  public void setNextTupleOperator(String currentOpId, TupleOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(nextOp);
    nextOps.put(currentOpId, nextOp);
  }

  @Override
  public RelationOperator getNextRelationOperator(String currentOpId) {
    // TODO Auto-generated method stub
    Operator nextOp = nextOps.get(currentOpId);
    if (nextOp != null && nextOp instanceof RelationOperator) {
      return (RelationOperator) nextOp;
    }
    throw new IllegalStateException();
  }

  @Override
  public TupleOperator getNextTupleOperator(String currentOpId) {
    // TODO Auto-generated method stub
    Operator nextOp = nextOps.get(currentOpId);
    if (nextOp != null && nextOp instanceof TupleOperator) {
      return (TupleOperator) nextOp;
    }
    throw new IllegalStateException();
  }

  @Override
  public Operator getNextTimeoutOperator(String currentOpId) {
    // TODO Auto-generated method stub
    return nextOps.get(currentOpId);
  }

  @Override
  public Iterator<Operator> iterator() {
    // TODO Auto-generated method stub
    return operators.values().iterator();
  }
}

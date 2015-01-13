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

package org.apache.samza.sql.operators.relation;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.operators.factory.SimpleOperator;


/**
 * This class defines an example build-in operator for a join operator between two relations.
 *
 */
public class Join extends SimpleOperator implements RelationOperator {

  /**
   * The input relations
   *
   */
  private List<Relation> inputs = null;

  /**
   * The output relation
   */
  private Relation output = null;

  /**
   * ctor that creates <code>Join</code> operator based on the specification.
   *
   * <p>This version of constructor is often used in an implementation of <code>SqlOperatorFactory</code>
   *
   * @param spec
   *     The <code>JoinSpec</code> object that specifies the join operator
   */
  public Join(JoinSpec spec) {
    super(spec);
  }

  /**
   * An alternative ctor that allows users to create a join operator randomly.
   *
   * @param id
   *     The identifier of the join operator
   * @param joinIns
   *     The list of input relation names of the join
   * @param joinOut
   *     The output relation name of the join
   * @param joinKeys
   *     The list of keys used in the join. Each entry in the <code>joinKeys</code> is the key name used in one of the input relations.
   *     The order of the <code>joinKeys</code> MUST be the same as their corresponding relation names in <code>joinIns</code>
   */
  public Join(String id, List<String> joinIns, String joinOut, List<String> joinKeys) {
    super(new JoinSpec(id, joinIns, joinOut, joinKeys));
  }

  private boolean hasPendingChanges() {
    return getPendingChanges() != null;
  }

  private Relation getPendingChanges() {
    // TODO Auto-generated method stub
    // return any pending changes that have not been processed yet
    return null;
  }

  private Relation getOutputChanges() {
    // TODO Auto-generated method stub
    return null;
  }

  private boolean hasOutputChanges() {
    // TODO Auto-generated method stub
    return getOutputChanges() != null;
  }

  private void join(Relation deltaRelation) {
    // TODO Auto-generated method stub
    // implement the join logic
    // 1. calculate the delta changes in <code>output</code>
    // 2. check output condition to see whether the current input should trigger an output
    // 3. set the output changes and pending changes
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    for (String relation : this.getSpec().getInputNames()) {
      inputs.add(initContext.getRelation(relation));
    }
    this.output = initContext.getRelation(this.getSpec().getOutputName());
  }

  @Override
  public void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception {
    if (hasPendingChanges()) {
      context.sendToNextRelationOperator(this.getId(), getPendingChanges());
    }
    context.sendToNextTimeoutOperator(this.getId(), currentSystemNano);
  }

  @Override
  public void process(Relation deltaRelation, RuntimeSystemContext context) throws Exception {
    // calculate join based on the input <code>deltaRelation</code>
    join(deltaRelation);
    if (hasOutputChanges()) {
      context.sendToNextRelationOperator(this.getId(), getOutputChanges());
    }
  }
}

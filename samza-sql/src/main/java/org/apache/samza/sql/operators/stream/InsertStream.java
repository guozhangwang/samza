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

package org.apache.samza.sql.operators.stream;

import java.util.Iterator;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.operators.spec.RelationOperatorSpec;
import org.apache.samza.sql.api.task.InitSystemContext;


public class InsertStream implements RelationOperator {

  private final InsertStreamSpec spec;
  private Relation relation = null;

  public InsertStream(InsertStreamSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void process(Relation deltaRelation, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    Iterator<Tuple> iterator = deltaRelation.iterator();
    for (; iterator.hasNext();) {
      Tuple tuple = iterator.next();
      if (!tuple.isDelete()) {
        context.sendToNextTupleOperator(this.spec.getId(), tuple);
      }
    }
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
    if (this.relation == null) {
      this.relation = initContext.getRelation(this.spec.getInputRelation());
    }
  }

  @Override
  public void timeout(long currentSystemNano, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    // assuming this operation does not have pending changes kept in memory
  }

  @Override
  public RelationOperatorSpec getSpec() {
    // TODO Auto-generated method stub
    return this.spec;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return this.spec.getId();
  }

}

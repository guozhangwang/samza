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
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.operators.factory.SimpleOperator;


/**
 * This class defines an example build-in operator for an istream operator that converts a relation to a stream
 *
 */
public class InsertStream extends SimpleOperator implements RelationOperator {
  /**
   * The <code>InsertStreamSpec</code> for this operator
   */
  private final InsertStreamSpec spec;

  /**
   * The time-varying relation that is to be converted into a stream
   */
  private Relation relation = null;

  /**
   * ctor that takes the specication of the object as input parameter
   *
   * <p>This version of constructor is often used in an implementation of <code>SqlOperatorFactory</code>
   *
   * @param spec
   *     The <code>InsertStreamSpec</code> specification of this operator
   */
  public InsertStream(InsertStreamSpec spec) {
    super(spec);
    this.spec = spec;
  }

  public InsertStream(String id, String input, String output) {
    super(new InsertStreamSpec(id, input, output));
    this.spec = (InsertStreamSpec) super.getSpec();
  }

  @Override
  public void process(Relation deltaRelation, RuntimeSystemContext context) throws Exception {
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
    if (this.relation == null) {
      this.relation = initContext.getRelation(this.spec.getInputRelation());
    }
  }

  @Override
  public void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception {
    // TODO Auto-generated method stub
    // assuming this operation does not have pending changes kept in memory
  }

}

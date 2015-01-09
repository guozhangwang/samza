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

package org.apache.samza.sql.operators.partition;

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.operators.factory.SimpleOperator;


public final class PartitionOp extends SimpleOperator implements TupleOperator {

  private final PartitionSpec spec;

  public PartitionOp(PartitionSpec spec) {
    // TODO Auto-generated constructor stub
    super(spec);
    this.spec = spec;
  }

  public PartitionOp(String id, String input, String output, String parKey, int parNum) {
    super(new PartitionSpec(id, input, output, parKey, parNum));
    this.spec = (PartitionSpec) super.getSpec();
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
    // No need to initialize store since all inputs are immediately send out
  }

  @Override
  public void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  public void process(Tuple tuple, RuntimeSystemContext context) throws Exception {
    // TODO Auto-generated method stub
    context.sendToNextTupleOperator(this.spec.getId(), this.setPartitionKey(tuple));
  }

  private Tuple setPartitionKey(Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    // This should set the partition key to <code>OutgoingMessageEnvelope</code> and return
    return tuple;
  }

}

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

import org.apache.samza.sql.api.data.OutgoingMessageTuple;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;


/**
 * This is an example build-in operator that performs a simple stream re-partition operation.
 *
 */
public final class PartitionOp extends SimpleOperator implements TupleOperator {

  /**
   * The specification of this <code>PartitionOp</code>
   *
   */
  private final PartitionSpec spec;

  /**
   * ctor that takes the <code>PartitionSpec</code> object as input.
   *
   * <p>This version of constructor is often used by an implementation class of <code>SqlOperatorFactory</code>
   *
   * @param spec
   *     The <code>PartitionSpec</code> object
   */
  public PartitionOp(PartitionSpec spec) {
    super(spec);
    this.spec = spec;
  }

  /**
   * A simplified constructor that allow users to randomly create <code>PartitionOp</code>
   *
   * @param id
   *     The identifier of this operator
   * @param input
   *     The input stream name of this operator
   * @param system
   *     The output system name of this operator
   * @param output
   *     The output stream name of this operator
   * @param parKey
   *     The partition key used for the output stream
   * @param parNum
   *     The number of partitions used for the output stream
   */
  public PartitionOp(String id, String input, String system, String output, String parKey, int parNum) {
    super(new PartitionSpec(id, input, new SystemStream(system, output), parKey, parNum));
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
    context.sendToNextTupleOperator(this.getId(), this.setPartitionKey(tuple));
  }

  private OutgoingMessageTuple setPartitionKey(Tuple tuple) throws Exception {
    // This should set the partition key to <code>OutgoingMessageEnvelope</code> and return
    return new OutgoingMessageTuple() {
      private final OutgoingMessageEnvelope omsg = new OutgoingMessageEnvelope(PartitionOp.this.spec.getSystemStream(),
          tuple.getKey(), tuple.getField(PartitionOp.this.spec.getParKey()), tuple.getMessage());

      @Override
      public Object getMessage() {
        return this.omsg.getMessage();
      }

      @Override
      public boolean isDelete() {
        return false;
      }

      @Override
      public Object getField(String name) {
        return tuple.getField(name);
      }

      @Override
      public Object getKey() {
        return this.omsg.getKey();
      }

      @Override
      public String getStreamName() {
        return this.omsg.getSystemStream().getStream();
      }

      @Override
      public OutgoingMessageEnvelope getOutgoingMessageEnvelope() {
        return this.omsg;
      }

      @Override
      public SystemStream getSystemStream() {
        return this.omsg.getSystemStream();
      }

      @Override
      public Object getPartitionKey() {
        return this.omsg.getPartitionKey();
      }
    };
  }

}

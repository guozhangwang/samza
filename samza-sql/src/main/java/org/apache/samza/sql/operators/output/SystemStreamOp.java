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

package org.apache.samza.sql.operators.output;

import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.operators.spec.TupleOperatorSpec;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;


public class SystemStreamOp implements TupleOperator {
  private final SystemStreamSpec spec;

  public SystemStreamOp(SystemStreamSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  public SystemStreamOp(String id, StreamSpec input, SystemStream output) {
    this.spec = new SystemStreamSpec(id, input, output);
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void timeout(long currentSystemNano, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  public void process(Tuple tuple, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    RuntimeSystemContext sysCntx = context.getRuntimeContext();
    sysCntx.getMessageCollector().send(getOutputMessagingEnvelope(tuple, this.spec.getOutputStream()));
  }

  private OutgoingMessageEnvelope getOutputMessagingEnvelope(Tuple tuple, SystemStream sysStream) throws Exception {
    // TODO Auto-generated method stub
    return new OutgoingMessageEnvelope(sysStream, tuple.getMessage());
  }

  @Override
  public TupleOperatorSpec getSpec() {
    // TODO Auto-generated method stub
    return this.spec;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return this.spec.getId();
  }
}

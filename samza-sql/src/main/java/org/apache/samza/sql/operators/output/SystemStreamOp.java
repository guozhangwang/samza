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

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;


public class SystemStreamOp extends SimpleOperator implements TupleOperator {
  private final SystemStreamSpec spec;

  public SystemStreamOp(SystemStreamSpec spec) {
    // TODO Auto-generated constructor stub
    super(spec);
    this.spec = spec;
  }

  public SystemStreamOp(String id, String input, SystemStream output) {
    super(new SystemStreamSpec(id, input, output));
    this.spec = (SystemStreamSpec) super.getSpec();
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  public void process(Tuple tuple, RuntimeSystemContext context) throws Exception {
    // TODO Auto-generated method stub
    context.getMessageCollector().send(getOutputMessagingEnvelope(tuple, this.spec.getOutputStream()));
  }

  private OutgoingMessageEnvelope getOutputMessagingEnvelope(Tuple tuple, SystemStream sysStream) throws Exception {
    // TODO Auto-generated method stub
    return new OutgoingMessageEnvelope(sysStream, tuple.getMessage());
  }

}

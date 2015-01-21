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

package org.apache.samza.sql.data;

import org.apache.samza.sql.api.data.IncomingMessageTuple;
import org.apache.samza.system.IncomingMessageEnvelope;


public class SystemInputTuple implements IncomingMessageTuple {
  private final IncomingMessageEnvelope imsg;

  public SystemInputTuple(IncomingMessageEnvelope imsg) {
    this.imsg = imsg;
  }

  @Override
  public Object getMessage() {
    // TODO Auto-generated method stub
    return this.imsg.getMessage();
  }

  @Override
  public boolean isDelete() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object getField(String name) {
    // TODO Auto-generated method stub.
    return null;
  }

  @Override
  public Object getKey() {
    // TODO Auto-generated method stub
    return this.imsg.getKey();
  }

  @Override
  public String getStreamName() {
    // Format the system stream name s.t. it would be unique in the system
    return String.format("%s:%s", this.imsg.getSystemStreamPartition().getSystemStream().getSystem(), this.imsg
        .getSystemStreamPartition().getSystemStream().getStream());
  }

  @Override
  public IncomingMessageEnvelope getIncomingMessageEnvelope() {
    // TODO Auto-generated method stub
    return this.imsg;
  }

}

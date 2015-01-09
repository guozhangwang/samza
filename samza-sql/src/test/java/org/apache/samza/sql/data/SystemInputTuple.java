package org.apache.samza.sql.data;

import org.apache.samza.sql.api.data.IncomingMessageTuple;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;


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

  @Override
  public SystemStreamPartition getSystemStreamPartition() {
    // TODO Auto-generated method stub
    return this.imsg.getSystemStreamPartition();
  }

}

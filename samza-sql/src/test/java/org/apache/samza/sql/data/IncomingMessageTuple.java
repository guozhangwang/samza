package org.apache.samza.sql.data;

import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.system.IncomingMessageEnvelope;


public class IncomingMessageTuple implements Tuple {

  private final IncomingMessageEnvelope imsg;

  public IncomingMessageTuple(IncomingMessageEnvelope imsg) {
    // TODO Auto-generated constructor stub
    this.imsg = imsg;
  }

  @Override
  public boolean isDelete() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object getField(String name) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getMessage() {
    // TODO Auto-generated method stub
    return this.imsg.getMessage();
  }

  @Override
  public StreamSpec getStreamSpec() {
    // TODO Auto-generated method stub
    return null;
  }

}

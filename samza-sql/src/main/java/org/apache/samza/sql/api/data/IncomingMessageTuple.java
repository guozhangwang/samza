package org.apache.samza.sql.api.data;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;


public interface IncomingMessageTuple extends Tuple {
  public IncomingMessageEnvelope getIncomingMessageEnvelope();

  public SystemStreamPartition getSystemStreamPartition();

}

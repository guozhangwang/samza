package org.apache.samza.sql.api.data;

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;

public interface OutgoingMessageTuple extends Tuple {

  public OutgoingMessageEnvelope getOutgoingMessageEnvelope();

  public SystemStream getSystemStream();

  public Object getPartitionKey();

}

package org.apache.samza.sql.operators.output;

import org.apache.samza.sql.api.data.RelationStore;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.system.OutgoingMessageEnvelope;


public class SystemStreamOp implements TupleOperator {
  private final SystemStreamSpec spec;

  public SystemStreamOp(SystemStreamSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void init(RelationStore store) throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void timeout(long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  public void process(Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    this.spec.getContext().getMessageCollector().send(getOutputMessagingEnvelope(tuple));
  }

  private OutgoingMessageEnvelope getOutputMessagingEnvelope(Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    return new OutgoingMessageEnvelope(this.spec.getSystemStream(), tuple.getMessage());
  }

}

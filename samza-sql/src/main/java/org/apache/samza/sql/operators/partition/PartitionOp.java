package org.apache.samza.sql.operators.partition;

import org.apache.samza.sql.api.data.RelationStore;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.TupleTupleOperator;


public final class PartitionOp implements TupleTupleOperator {

  private final PartitionSpec spec;
  private TupleOperator nextOp = null;

  public PartitionOp(PartitionSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void init(RelationStore store) throws Exception {
    // TODO Auto-generated method stub
    // No need to initialize store since all inputs are immediately send out
  }

  @Override
  public void timeout(long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  public void process(Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    this.nextOp.process(this.setPartitionKey(tuple));
  }

  private Tuple setPartitionKey(Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    // This should set the partition key to <code>OutgoingMessageEnvelope</code> and return
    return tuple;
  }

  @Override
  public void setNextOp(TupleOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    this.nextOp = nextOp;
  }

}

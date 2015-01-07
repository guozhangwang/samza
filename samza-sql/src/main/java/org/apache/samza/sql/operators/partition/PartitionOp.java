package org.apache.samza.sql.operators.partition;

import java.util.List;

import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.InitSystemContext;


public final class PartitionOp implements TupleOperator {

  private final PartitionSpec spec;

  public PartitionOp(PartitionSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
    // No need to initialize store since all inputs are immediately send out
  }

  @Override
  public void timeout(long currentSystemNano, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    // NOOP or flush
  }

  @Override
  public void process(Tuple tuple, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    context.sendToNextTupleOperator(this.spec.getId(), this.setPartitionKey(tuple));
  }

  private Tuple setPartitionKey(Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    // This should set the partition key to <code>OutgoingMessageEnvelope</code> and return
    return tuple;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return this.spec.getId();
  }

  @Override
  public List<StreamSpec> getInputTuples() {
    // TODO Auto-generated method stub
    return this.spec.getInputSpec();
  }

}

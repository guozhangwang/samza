package org.apache.samza.sql.operators.output;

import java.util.List;

import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.OperatorSystemContext;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;


public class SystemStreamOp implements TupleOperator {
  private final SystemStreamSpec spec;
  private OperatorSystemContext sysCntx;

  public SystemStreamOp(SystemStreamSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
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
    this.sysCntx.getMessageCollector().send(getOutputMessagingEnvelope(tuple, this.sysCntx.getSystemStream()));
  }

  @Override
  public boolean isSystemOutput() {
    return true;
  }

  @Override
  public void setSystemContext(OperatorSystemContext sysContext) {
    this.sysCntx = sysContext;
  }

  private OutgoingMessageEnvelope getOutputMessagingEnvelope(Tuple tuple, SystemStream sysStream) throws Exception {
    // TODO Auto-generated method stub
    return new OutgoingMessageEnvelope(sysStream, tuple.getMessage());
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

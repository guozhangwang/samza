package org.apache.samza.sql.task;

import org.apache.samza.sql.api.data.OutgoingMessageTuple;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public class RoutableRuntimeContext implements RuntimeSystemContext {

  private final MessageCollector collector;
  private final TaskCoordinator coordinator;
  private final OperatorRoutingContext rteCntx;

  public RoutableRuntimeContext(MessageCollector collector, TaskCoordinator coordinator, OperatorRoutingContext rteCntx) {
    this.collector = collector;
    this.coordinator = coordinator;
    this.rteCntx = rteCntx;
  }

  @Override
  public void sendToNextRelationOperator(String currentOpId, Relation deltaRelation) throws Exception {
    // TODO Auto-generated method stub
    this.rteCntx.getNextRelationOperator(currentOpId).process(deltaRelation, this);
  }

  @Override
  public void sendToNextTupleOperator(String currentOpId, Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    if (this.rteCntx.getNextTupleOperator(currentOpId) != null) {
      // by default, always send to the next operator
      this.rteCntx.getNextTupleOperator(currentOpId).process(tuple, this);
    } else if (tuple instanceof OutgoingMessageTuple) {
      // if there is no next operator, check whether the tuple is an OutgoingMessageTuple
      this.collector.send(((OutgoingMessageTuple) tuple).getOutgoingMessageEnvelope());
    }
    throw new IllegalStateException("No next tuple operator found and the tuple is not an OutgoingMessageTuple");
  }

  @Override
  public void sendToNextTimeoutOperator(String currentOpId, long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
    this.rteCntx.getNextTimeoutOperator(currentOpId).timeout(currentSystemNano, this);
  }

}

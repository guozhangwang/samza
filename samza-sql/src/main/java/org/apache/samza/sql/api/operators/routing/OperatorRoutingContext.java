package org.apache.samza.sql.api.operators.routing;

import java.util.List;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;


public interface OperatorRoutingContext {
  public void setSystemInputOperator(TupleOperator inputOp) throws Exception;

  public MultiValueMap getSystemInputOps();

  public List<Operator> getSystemOutputOps();

  public void setNextRelationOperator(String currentOpId, RelationOperator nextOp) throws Exception;

  public void setNextTupleOperator(String currentOpId, TupleOperator nextOp) throws Exception;

  public RelationOperator getNextRelationOperator(String currentOpId);

  public TupleOperator getNextTupleOperator(String currentOpId);

  public Operator getNextTimeoutOperator(String currentOpId);

  public default void sendToNextRelationOperator(String currentOpId, Relation deltaRelation) throws Exception {
    this.getNextRelationOperator(currentOpId).process(deltaRelation, this);
  };

  public default void sendToNextTupleOperator(String currentOpId, Tuple tuple) throws Exception {
    this.getNextTupleOperator(currentOpId).process(tuple, this);
  };

  public default void sendToNextTimeoutOperator(String currentOpId, long currentSystemNano) throws Exception {
    this.getNextTimeoutOperator(currentOpId).timeout(currentSystemNano, this);
  }

}

package org.apache.samza.sql.operators.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;


public class SqlRoutingContextManager implements OperatorRoutingContext {
  private Map<String, Operator> operators = new HashMap<String, Operator>();
  private Map<String, Operator> nextOps = new HashMap<String, Operator>();
  private MultiValueMap sysInputOps = new MultiValueMap();
  private List<Operator> sysOutputOps = new ArrayList<Operator>();

  private void addOperator(String id, Operator op) {
    operators.put(id, op);
    if (op.isSystemOutput()) {
      sysOutputOps.add(op);
    }
  }

  @Override
  public void setSystemInputOperator(TupleOperator inputOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(inputOp.getId(), inputOp);
    for (StreamSpec spec : inputOp.getInputTuples()) {
      sysInputOps.put(spec, inputOp);
    }
  }

  @Override
  public MultiValueMap getSystemInputOps() {
    // TODO Auto-generated method stub
    return this.sysInputOps;
  }

  @Override
  public List<Operator> getSystemOutputOps() {
    // TODO Auto-generated method stub
    return this.sysOutputOps;
  }

  @Override
  public void setNextRelationOperator(String currentOpId, RelationOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(currentOpId, nextOp);
    nextOps.put(currentOpId, nextOp);
  }

  @Override
  public void setNextTupleOperator(String currentOpId, TupleOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    addOperator(currentOpId, nextOp);
    nextOps.put(currentOpId, nextOp);
  }

  @Override
  public RelationOperator getNextRelationOperator(String currentOpId) {
    // TODO Auto-generated method stub
    Operator nextOp = nextOps.get(currentOpId);
    if (nextOp != null && nextOp instanceof RelationOperator) {
      return (RelationOperator) nextOp;
    }
    throw new IllegalStateException();
  }

  @Override
  public TupleOperator getNextTupleOperator(String currentOpId) {
    // TODO Auto-generated method stub
    Operator nextOp = nextOps.get(currentOpId);
    if (nextOp != null && nextOp instanceof TupleOperator) {
      return (TupleOperator) nextOp;
    }
    throw new IllegalStateException();
  }

  @Override
  public Operator getNextTimeoutOperator(String currentOpId) {
    // TODO Auto-generated method stub
    return nextOps.get(currentOpId);
  }

}

package org.apache.samza.sql.operators.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;


public class ManualRoutingContext extends AutoRoutingContext {
  private Map<String, List<Object>> opOutputs = new HashMap<String, List<Object>>();

  private void addToOutput(String key, Object output) throws Exception {
    // for manual routing context, only record the output
    if (opOutputs.get(key) == null) {
      opOutputs.put(key, new ArrayList<Object>());
    }
    List<Object> list = opOutputs.get(key);
    list.add(output);
  }

  @Override
  public void sendToNextRelationOperator(String currentOpId, Relation deltaRelation) throws Exception {
    addToOutput(currentOpId, deltaRelation);
  }

  @Override
  public void sendToNextTupleOperator(String currentOpId, Tuple tuple) throws Exception {
    addToOutput(currentOpId, tuple);
  }

  @Override
  public void sendToNextTimeoutOperator(String currentOpId, long sysTimeNano) throws Exception {
    // Nothing to be done for timeout here, since the timeout of the next operator should be called explicitly
  }

  public List<Object> getOutput(String opId) {
    return opOutputs.get(opId);
  }

}

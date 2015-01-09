package org.apache.samza.sql.task;

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public class StoredRuntimeContext implements RuntimeSystemContext {

  private final MessageCollector collector;
  private final TaskCoordinator coordinator;
  private final KeyValueStore<String, List<Object>> outputStore;

  public StoredRuntimeContext(MessageCollector collector, TaskCoordinator coordinator,
      KeyValueStore<String, List<Object>> store) {
    this.collector = collector;
    this.coordinator = coordinator;
    this.outputStore = store;
  }

  @Override
  public MessageCollector getMessageCollector() {
    // TODO Auto-generated method stub
    return collector;
  }

  @Override
  public TaskCoordinator getTaskCoordinator() {
    // TODO Auto-generated method stub
    return coordinator;
  }

  @Override
  public void sendToNextRelationOperator(String currentOpId, Relation deltaRelation) throws Exception {
    // TODO Auto-generated method stub
    saveOutput(currentOpId, deltaRelation);
  }

  @Override
  public void sendToNextTupleOperator(String currentOpId, Tuple tuple) throws Exception {
    // TODO Auto-generated method stub
    saveOutput(currentOpId, tuple);
  }

  @Override
  public void sendToNextTimeoutOperator(String currentOpId, long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
  }

  public List<Object> removeOutput(String id) {
    // TODO Auto-generated method stub
    List<Object> output = outputStore.get(id);
    outputStore.delete(id);
    return output;
  }

  private void saveOutput(String currentOpId, Object output) {
    // TODO Auto-generated method stub
    if (this.outputStore.get(currentOpId) == null) {
      this.outputStore.put(currentOpId, new ArrayList<Object>());
    }
    List<Object> outputs = this.outputStore.get(currentOpId);
    outputs.add(output);
  }

}

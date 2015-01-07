package org.apache.samza.sql.store;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.operators.window.WindowState;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;


public class SqlContextManager implements InitSystemContext {

  private final TaskContext context;

  public SqlContextManager(TaskContext context) {
    // TODO Auto-generated constructor stub
    this.context = context;
  }

  @Override
  public Relation getRelation(RelationSpec spec) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<WindowState> getWindowStates(String wndName) {
    // TODO Auto-generated method stub
    return null;
  }

  public SystemStream getSystemStream() {
    return this.context.getSystemStreamPartitions().iterator().next().getSystemStream();
  }

}

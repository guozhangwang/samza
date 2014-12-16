package org.apache.samza.sql.store;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.data.RelationStore;
import org.apache.samza.sql.operators.window.WindowState;
import org.apache.samza.task.TaskContext;


public class SQLRelationStore implements RelationStore {

  public SQLRelationStore(TaskContext context) {
    // TODO Auto-generated constructor stub
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

}

package org.apache.samza.sql.operators.window;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.RelationStore;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleRelationOperator;


public class BoundedTimeWindow implements TupleRelationOperator {

  private final WindowSpec spec;

  private Relation relation = null;

  private RelationOperator nextOp = null;

  private List<WindowState> windowStates = null;

  public BoundedTimeWindow(WindowSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void process(Tuple tuple) throws Exception {
    // TODO Process each incoming tuple
    // for each tuple, this will evaluate the incoming tuple and update the window states.
    // If the window states allow generating output, calculate the delta changes in
    // the window relation and execute the relation operation <code>nextOp</code>
    updateWindow(tuple);
    processWindowChanges();
  }

  private void processWindowChanges() throws Exception {
    // TODO Auto-generated method stub
    if (windowStateChange()) {
      this.nextOp.process(getWindowChanges());
    }
  }

  private Relation getWindowChanges() {
    // TODO Auto-generated method stub
    return null;
  }

  private boolean windowStateChange() {
    // TODO Auto-generated method stub
    return getWindowChanges() != null;
  }

  private void updateWindow(Tuple tuple) {
    // TODO Auto-generated method stub
    // The window states are updated here
    // And the correpsonding deltaChanges is also calculated here.
  }

  @Override
  public void timeout(long currentSystemNano) throws Exception {
    // TODO timeout needs to be implemented per window spec, default is doing nothing
    updateWindowTimeout();
    processWindowChanges();
    this.nextOp.timeout(currentSystemNano);
  }

  private void updateWindowTimeout() {
    // TODO Auto-generated method stub

  }

  @Override
  public void init(RelationStore store) throws Exception {
    // TODO Auto-generated method stub
    if (this.relation == null) {
      this.relation = store.getRelation(this.spec.getOutputSpec());
      this.windowStates = store.getWindowStates(this.spec.getId());
    }
  }

  @Override
  public void setNextOp(RelationOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    this.nextOp = nextOp;
  }
}

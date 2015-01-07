package org.apache.samza.sql.operators.window;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.InitSystemContext;


public class BoundedTimeWindow implements TupleOperator {

  private final WindowSpec spec;

  private Relation relation = null;

  private List<WindowState> windowStates = null;

  public BoundedTimeWindow(WindowSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void process(Tuple tuple, OperatorRoutingContext context) throws Exception {
    // TODO Process each incoming tuple
    // for each tuple, this will evaluate the incoming tuple and update the window states.
    // If the window states allow generating output, calculate the delta changes in
    // the window relation and execute the relation operation <code>nextOp</code>
    updateWindow(tuple);
    processWindowChanges(context);
  }

  private void processWindowChanges(OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    if (windowStateChange()) {
      context.sendToNextRelationOperator(this.spec.getId(), getWindowChanges());
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
  public void timeout(long currentSystemNano, OperatorRoutingContext context) throws Exception {
    // TODO timeout needs to be implemented per window spec, default is doing nothing
    updateWindowTimeout();
    processWindowChanges(context);
    context.sendToNextTimeoutOperator(this.spec.getId(), currentSystemNano);
  }

  private void updateWindowTimeout() {
    // TODO Auto-generated method stub
    // The window states are updated here
    // And the correpsonding deltaChanges is also calculated here.
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
    if (this.relation == null) {
      this.relation = initContext.getRelation(this.spec.getWindowedRelationSpec());
      this.windowStates = initContext.getWindowStates(this.spec.getId());
    }
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

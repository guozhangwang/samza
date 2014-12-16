package org.apache.samza.sql.operators.relation;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.data.RelationStore;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.RelationRelationOperator;


public class Join implements RelationRelationOperator {

  private final JoinSpec spec;

  private List<Relation> inputs = null;

  private Relation output = null;

  private RelationOperator nextOp = null;

  public Join(JoinSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void init(RelationStore store) throws Exception {
    // TODO Auto-generated method stub
    for (RelationSpec relationSpec : this.spec.getInputSpecs()) {
      inputs.add(store.getRelation(relationSpec));
    }
    this.output = store.getRelation(this.spec.getOutputSpec());
  }

  @Override
  public void timeout(long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
    if (hasPendingChanges()) {
      this.nextOp.process(getPendingChanges());
    }
    this.nextOp.timeout(currentSystemNano);
  }

  private boolean hasPendingChanges() {
    // TODO Auto-generated method stub
    return getPendingChanges() != null;
  }

  private Relation getPendingChanges() {
    // TODO Auto-generated method stub
    // return any pending changes that have not been processed yet
    return null;
  }

  @Override
  public void process(Relation deltaRelation) throws Exception {
    // TODO Auto-generated method stub
    // calculate join based on the input <code>deltaRelation</code>
    join(deltaRelation);
    if (hasOutputChanges()) {
      this.nextOp.process(getOutputChanges());
    }
  }

  private Relation getOutputChanges() {
    // TODO Auto-generated method stub
    return null;
  }

  private boolean hasOutputChanges() {
    // TODO Auto-generated method stub
    return getOutputChanges() != null;
  }

  private void join(Relation deltaRelation) {
    // TODO Auto-generated method stub
    // implement the join logic
    // 1. calculate the delta changes in <code>output</code>
    // 2. check output condition to see whether the current input should trigger an output
    // 3. set the output changes and pending changes
  }

  @Override
  public void setNextOp(RelationOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    this.nextOp = nextOp;
  }

}

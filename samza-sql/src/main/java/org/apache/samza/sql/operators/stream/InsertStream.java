package org.apache.samza.sql.operators.stream;

import java.util.Iterator;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.RelationStore;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.RelationTupleOperator;
import org.apache.samza.sql.api.operators.TupleOperator;


public class InsertStream implements RelationTupleOperator {

  private final InsertStreamSpec spec;
  private Relation relation = null;
  private TupleOperator nextOp;

  public InsertStream(InsertStreamSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void process(Relation deltaRelation) throws Exception {
    // TODO Auto-generated method stub
    Iterator<Tuple> iterator = deltaRelation.iterator();
    for (; iterator.hasNext();) {
      Tuple tuple = iterator.next();
      if (!tuple.isDelete()) {
        this.nextOp.process(tuple);
      }
    }
  }

  @Override
  public void init(RelationStore store) throws Exception {
    // TODO Auto-generated method stub
    if (this.relation == null) {
      this.relation = store.getRelation(this.spec.getInputSpecs().get(0));
    }
  }

  @Override
  public void timeout(long currentSystemNano) throws Exception {
    // TODO Auto-generated method stub
    // assuming this operation does not have pending changes kept in memory
  }

  @Override
  public void setNextOp(TupleOperator nextOp) throws Exception {
    // TODO Auto-generated method stub
    this.nextOp = nextOp;
  }

}

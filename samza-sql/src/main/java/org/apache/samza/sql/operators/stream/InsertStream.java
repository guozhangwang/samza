package org.apache.samza.sql.operators.stream;

import java.util.Iterator;
import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.InitSystemContext;


public class InsertStream implements RelationOperator {

  private final InsertStreamSpec spec;
  private Relation relation = null;

  public InsertStream(InsertStreamSpec spec) {
    // TODO Auto-generated constructor stub
    this.spec = spec;
  }

  @Override
  public void process(Relation deltaRelation, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    Iterator<Tuple> iterator = deltaRelation.iterator();
    for (; iterator.hasNext();) {
      Tuple tuple = iterator.next();
      if (!tuple.isDelete()) {
        context.sendToNextTupleOperator(this.spec.getId(), tuple);
      }
    }
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
    if (this.relation == null) {
      this.relation = initContext.getRelation(this.spec.getInputRelation());
    }
  }

  @Override
  public void timeout(long currentSystemNano, OperatorRoutingContext context) throws Exception {
    // TODO Auto-generated method stub
    // assuming this operation does not have pending changes kept in memory
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return this.spec.getId();
  }

  @Override
  public List<RelationSpec> getInputRelations() {
    // TODO Auto-generated method stub
    return this.spec.getInputSpecs();
  }

}

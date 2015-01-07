package org.apache.samza.sql.operators.relation;

import java.util.List;

import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.operators.spec.RelationOperatorSpec;


public class JoinSpec implements RelationOperatorSpec {

  @Override
  public List<RelationSpec> getInputSpecs() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return null;
  }

  public RelationSpec getJoinRelation() {
    return null;
  }
}

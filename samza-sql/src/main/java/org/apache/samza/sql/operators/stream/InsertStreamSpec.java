package org.apache.samza.sql.operators.stream;

import java.util.List;

import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.operators.spec.RelationOperatorSpec;


public class InsertStreamSpec implements RelationOperatorSpec {

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<RelationSpec> getInputSpecs() {
    // TODO Auto-generated method stub
    return null;
  }

  public RelationSpec getInputRelation() {
    return this.getInputSpecs().get(0);
  }

  public StreamSpec getStreamSpec() {
    // TODO Auto-generated method stub
    return null;
  }

}

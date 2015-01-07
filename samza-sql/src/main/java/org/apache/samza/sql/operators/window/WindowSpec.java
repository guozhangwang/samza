package org.apache.samza.sql.operators.window;

import java.util.List;

import org.apache.samza.sql.api.data.RelationSpec;
import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.operators.spec.TupleOperatorSpec;


public class WindowSpec implements TupleOperatorSpec {

  @Override
  public List<StreamSpec> getInputSpec() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return null;
  }

  public RelationSpec getWindowedRelationSpec() {
    // TODO Auto-generated method stub
    return null;
  }

}

package org.apache.samza.sql.operators.output;

import org.apache.samza.sql.api.data.StreamSpec;
import org.apache.samza.sql.api.operators.spec.TupleOperatorSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.SqlTaskContext;


public class SystemStreamSpec implements TupleOperatorSpec {
  private final SqlTaskContext context;

  public SystemStreamSpec(SqlTaskContext sqlContext) {
    // TODO Auto-generated constructor stub
    this.context = sqlContext;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StreamSpec getInputSpec() {
    // TODO Auto-generated method stub
    return null;
  }

  public SqlTaskContext getContext() {
    return this.context;
  }

  public SystemStream getSystemStream() {
    // TODO Auto-generated method stub
    return null;
  }
}

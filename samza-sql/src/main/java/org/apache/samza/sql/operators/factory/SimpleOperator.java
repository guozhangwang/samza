package org.apache.samza.sql.operators.factory;

import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.spec.OperatorSpec;


public abstract class SimpleOperator implements Operator {
  private final OperatorSpec spec;

  public SimpleOperator(OperatorSpec spec) {
    this.spec = spec;
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return this.spec.getId();
  }

  @Override
  public OperatorSpec getSpec() {
    // TODO Auto-generated method stub
    return this.spec;
  }

}

package org.apache.samza.sql.api.operators.spec;

public interface OperatorSpec {
  /**
   * interface method that returns the unique ID of the operator in a task
   *
   * @return
   *     the unique ID of the <code>Operator</code> object
   */
  public String getId();
}

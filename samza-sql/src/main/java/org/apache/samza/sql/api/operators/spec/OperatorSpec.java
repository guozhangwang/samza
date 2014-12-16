package org.apache.samza.sql.api.operators.spec;

/**
 * This class defines a generic interface for specification of an operator
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface OperatorSpec {

  /**
   * The unique identifier of the operator in a task.
   *
   * @return
   *     The unique identifier of the operator
   */
  public String getId();

}

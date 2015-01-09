package org.apache.samza.sql.api.operators.spec;

import java.util.List;


public interface OperatorSpec {
  /**
   * interface method that returns the unique ID of the operator in a task
   *
   * @return
   *     the unique ID of the <code>Operator</code> object
   */
  public String getId();

  /**
   * Access method to the list of input relation specifications
   *
   * @return
   *     A list of <code>RelationSpec</code> that is the input of the corresponding operator
   */
  public List<String> getInputNames();

  /**
   * Access method to the list of input relation specifications
   *
   * @return
   *     A list of <code>RelationSpec</code> that is the input of the corresponding operator
   */
  public String getOutputName();
}

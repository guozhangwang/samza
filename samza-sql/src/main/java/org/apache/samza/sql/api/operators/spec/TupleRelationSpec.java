package org.apache.samza.sql.api.operators.spec;

import org.apache.samza.sql.api.data.RelationSpec;


/**
 * This interface defines the APIs for specification of <code>TupleRelationOperator</code>, such as windowing operator.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface TupleRelationSpec extends TupleOperatorSpec {
  /**
   * Access method to the specification of the output relation
   *
   * @return
   *     The specification object that defines the output relation
   */
  public RelationSpec getOutputSpec();
}

package org.apache.samza.sql.api.operators.spec;

import org.apache.samza.sql.api.data.StreamSpec;


/**
 * This interface defines the APIs for specification of <code>RelationTupleOperator</code>.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface RelationTupleSpec extends RelationOperatorSpec {
  /**
   * Get the specification of the output stream of the <code>RelationTupleOperator</code>
   *
   * @return
   *     The specification object of the output stream
   */
  public StreamSpec getOutputSpec();
}

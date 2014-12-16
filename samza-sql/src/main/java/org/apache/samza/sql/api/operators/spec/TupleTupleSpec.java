package org.apache.samza.sql.api.operators.spec;

import org.apache.samza.sql.api.data.StreamSpec;


/**
 * This interface defines the APIs for specification of <code>TupleTupleOperator</code>, such as partition operators.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface TupleTupleSpec extends TupleOperatorSpec {
  /**
   * Get the specification of the output stream of the <code>TupleTupleOperator</code>
   *
   * @return
   *     The specification object of the output stream
   */
  public StreamSpec getOutputSpec();
}

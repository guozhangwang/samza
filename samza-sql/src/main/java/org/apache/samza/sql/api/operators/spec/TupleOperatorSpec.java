package org.apache.samza.sql.api.operators.spec;

import java.util.List;

import org.apache.samza.sql.api.data.StreamSpec;


/**
 * This class defines a generic interface for specification of a <code>TupleOperator</code> that takes <code>Tuple</code> as input
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface TupleOperatorSpec extends OperatorSpec {
  /**
   * Get the specification of the input stream of this operator
   *
   * @return
   *     The specification object of the input stream
   */
  public List<StreamSpec> getInputSpec();
}

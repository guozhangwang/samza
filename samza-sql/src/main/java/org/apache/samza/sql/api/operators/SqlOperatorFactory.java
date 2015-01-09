package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.operators.spec.OperatorSpec;


/**
 * This class defines the interface of SQL operator factory, which creates the following operators:
 * <ul>
 * <li> <code>RelationOperator</code> that takes <code>Relation</code> as both input and output
 * <li> <code>TupleOperator</code> that takes <code>Tuple</code> as input w/o next operator
 * </ul>
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface SqlOperatorFactory {

  /**
   * Interface method to create/get the relation-to-relation operator object
   *
   * @param spec
   *     The specification of the relation-to-relation operator object
   * @return
   *     The relation-to-relation operator object
   */
  public RelationOperator getRelationOperator(OperatorSpec spec);

  /**
   * Interface method to create/get the tuple operator object
   *
   * @param spec
   *     The specification of the tuple operator object
   * @return
   *     The tuple operator object
   */
  public TupleOperator getTupleOperator(OperatorSpec spec);

}

package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.operators.spec.RelationOperatorSpec;
import org.apache.samza.sql.api.operators.spec.RelationRelationSpec;
import org.apache.samza.sql.api.operators.spec.RelationTupleSpec;
import org.apache.samza.sql.api.operators.spec.TupleOperatorSpec;
import org.apache.samza.sql.api.operators.spec.TupleRelationSpec;
import org.apache.samza.sql.api.operators.spec.TupleTupleSpec;


/**
 * This class defines the interface of SQL operator factory, which creates the following operators:
 * <ul>
 * <li> <code>RelationOperator</code> that takes <code>Relation</code> as input w/o next operator
 * <li> <code>RelationRelationOperator</code> that takes <code>Relation</code> as both input and output
 * <li> <code>RelationTupleOperator</code> that takes <code>Relation</code> as input and a sequence of <code>Tuple</code> as output
 * <li> <code>TupleOperator</code> that takes <code>Tuple</code> as input w/o next operator
 * <li> <code>TupleRelationOperator</code> that takes <code>Tuple</code> as input and <code>Relation</code> as output
 * <li> <code>TupleTupleOperator</code> that takes <code>Tuple</code> as both input and output
 * </ul>
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface SqlOperatorFactory {
  /**
   * Interface method to create/get the relation operator object
   *
   * @param spec
   *     The specification of the relation operator object
   * @return
   *     The relation operator object
   */
  public RelationOperator getRelationOperator(RelationOperatorSpec spec);

  /**
   * Interface method to create/get the relation-to-relation operator object
   *
   * @param spec
   *     The specification of the relation-to-relation operator object
   * @return
   *     The relation-to-relation operator object
   */
  public RelationRelationOperator getRelationRelationOperator(RelationRelationSpec spec);

  /**
   * Interface method to create/get the relation-to-tuple operator object
   *
   * @param spec
   *     The specification of the relation-to-tuple operator object
   * @return
   *     The relation-to-tuple operator object
   */
  public RelationTupleOperator getRelationTupleOperator(RelationTupleSpec spec);

  /**
   * Interface method to create/get the tuple operator object
   *
   * @param spec
   *     The specification of the tuple operator object
   * @return
   *     The tuple operator object
   */
  public TupleOperator getTupleOperator(TupleOperatorSpec spec);

  /**
   * Interface method to create/get the tuple-to-relation operator object
   *
   * @param spec
   *     The specification of the tuple-to-relation operator object
   * @return
   *     The tuple-to-relation operator object
   */
  public TupleRelationOperator getTupleRelationOperator(TupleRelationSpec spec);

  /**
   * Interface method to create/get the tuple-to-tuple operator object
   *
   * @param spec
   *     The specification of tuple-to-tuple operator object
   * @return
   *     The tuple-to-tuple operator object
   */
  public TupleTupleOperator getTupleTupleOperator(TupleTupleSpec spec);
}

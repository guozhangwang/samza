package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.data.Relation;


/**
 * This class defines the interface <code>RelationOp</code> and <code>StreamOp</code> classes should support.
 * All classes implement this interface should process a certain relation algebra with input relation changes.
 *
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface RelationOperator extends Operator {

  /**
   * interface method to perform a relational algebra on a set of relations.
   * <p> The output of this method is always driven by a change in a certain input relation,
   * which enforces a certain order of execution on the incoming changes. When task crashes
   * and recovers, this method expects to see the same sequence of input changes in the recovery
   * to produce the same sequence of output values.
   *
   * @param deltaRelation
   *     the changed rows in the input relation, including the inserts/deletes/updates
   * @throws Exception
   *     Throws exception if failed
   */
  public void process(Relation deltaRelation) throws Exception;

}

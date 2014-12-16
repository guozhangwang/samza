package org.apache.samza.sql.api.operators;

/**
 * This class defines additional interface method for a relation-to-tuple operator, which takes a relation as input and generate a sequence of tuples as an output
 *
 * @author Yi Pan{yipan@linkedin.com}
 *
 */
public interface RelationTupleOperator extends RelationOperator {

  /**
   * This method sets the next operator of the <code>RelationTupleOperator</code>
   *
   * @param nextOp
   *     The tuple operator object that is to be triggered to process the output of this operator
   * @throws Exception
   *     Throws exception if failed
   */
  public void setNextOp(TupleOperator nextOp) throws Exception;
}

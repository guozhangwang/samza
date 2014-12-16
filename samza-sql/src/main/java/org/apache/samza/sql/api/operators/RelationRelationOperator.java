package org.apache.samza.sql.api.operators;

/**
 * This class defines additional interface method for a relation-to-relation operator, which takes a relation as input and generate a relation as an output
 *
 * @author Yi Pan{yipan@linkedin.com}
 *
 */
public interface RelationRelationOperator extends RelationOperator {

  /**
   * This method sets the next operator of the <code>RelationRelationOperator</code>
   *
   * @param nextOp
   *     The relation operator object that is to be triggered to process the output of this operator
   * @throws Exception
   *     Throws exception if failed
   */
  public void setNextOp(RelationOperator nextOp) throws Exception;
}

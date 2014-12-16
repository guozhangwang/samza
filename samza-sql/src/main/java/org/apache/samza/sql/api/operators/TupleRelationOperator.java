package org.apache.samza.sql.api.operators;

/**
 * This interface class defines additional method for a tuple operator that generates an relation object as the output
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface TupleRelationOperator extends TupleOperator {
  /**
   * Set the next relation operator object that takes the output of this operator
   *
   * @param nextOp
   *     The relation operator object that is to be triggered to process the output of this operator
   * @throws Exception
   *     Throws exception if failed
   */
  public void setNextOp(RelationOperator nextOp) throws Exception;

}

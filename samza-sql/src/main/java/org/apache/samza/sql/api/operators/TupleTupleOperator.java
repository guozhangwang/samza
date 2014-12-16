package org.apache.samza.sql.api.operators;

/**
 * This interface class defines additional method for a tuple operator that generates another tuple object as the output
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface TupleTupleOperator extends TupleOperator {
  /**
   * Set the next tuple operator object that takes the output of this operator
   *
   * @param nextOp
   *     The tuple operator object that is to be triggered to process the output of this operator
   * @throws Exception
   *     Throws exception if failed
   */
  public void setNextOp(TupleOperator nextOp) throws Exception;

}

package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.data.Tuple;


/**
 * This class defines the interface class that processes incoming tuples from a single input stream.
 *
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface TupleOperator extends Operator {
  /**
   * interface method to process on an input tuple.
   * <p>This interface method is expected to be invoked when i) converting stream to relation; Or ii) repartitioning a stream
   *
   * @param tuple
   *     The input tuple, which has the incoming message from a stream
   * @throws Exception
   *     Throws exception if failed
   */
  public void process(Tuple tuple) throws Exception;

}

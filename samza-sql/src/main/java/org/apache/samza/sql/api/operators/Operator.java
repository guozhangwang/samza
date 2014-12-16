package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.data.RelationStore;


/**
 * This class defines the timeout interface all operator classes should support.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface Operator {
  /**
   * interface method to initialize the operator via the <code>RelationStore</code>
   *
   * @param store
   *     The store object that provides interface to recover states from the underlying storage layer
   * @throws Exception
   *     Throw exception if failed to initialize the store
   */
  public void init(RelationStore store) throws Exception;

  /**
   * interface method that is to be called when timer expires instead of relation changes are calculated
   *
   * @param currentSystemNano
   *     the current system time in nano-second
   * @throws Exception
   *     Throws exception if failed
   */
  public void timeout(long currentSystemNano) throws Exception;

}

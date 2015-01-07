package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.operators.routing.OperatorRoutingContext;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.OperatorSystemContext;


/**
 * This class defines the initialize interface for operator classes.
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
  public void init(InitSystemContext initContext) throws Exception;

  /**
   * interface method that is to be called when timer expires instead of relation changes are calculated
   *
   * @param currentSystemNano
   *     the current system time in nano-second
   * @throws Exception
   *     Throws exception if failed
   */
  public void timeout(long currentSystemNano, OperatorRoutingContext context) throws Exception;

  public String getId();

  public default boolean isSystemOutput() {
    return false;
  }

  public default void setSystemContext(OperatorSystemContext opCntx) {
    throw new UnsupportedOperationException();
  }
}

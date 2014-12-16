package org.apache.samza.task;



/**
 * This class extends the <code>TaskContext</code> class to include more access methods to task context in SQL operators
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface SqlTaskContext extends TaskContext {

  /**
   * This method sets up the <code>SqlTaskContext</code>
   *
   * @param collector
   *     The <code>MessageCollector</code> to be used in SQL operators
   * @param coordinator
   *     The <code>TaskCoordinator</code> to be used in SQL operators
   */
  public void setup(MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Access method to the <code>MessageCollector</code>
   *
   * @return
   *     The message collector
   */
  public MessageCollector getMessageCollector();

  /**
   * Access method to the <code>TaskCoordinator</code>
   *
   * @return
   *     The task coordinator object
   */
  public TaskCoordinator getCoordinator();

  /**
   * This method resets the <code>MessageCollector</code> and <code>TaskCoordinator</code> objects to null
   *
   */
  public void reset();
}

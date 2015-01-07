package org.apache.samza.sql.api.task;

import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


public interface OperatorSystemContext {

  public MessageCollector getMessageCollector();

  public TaskCoordinator getTaskCoordinator();

  public SystemStream getSystemStream();
}

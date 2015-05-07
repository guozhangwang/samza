package org.apache.samza.sql.api.operators;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;

public interface OperatorCallback {

  Tuple preProcess(Tuple tuple, SqlMessageCollector collector, TaskCoordinator coordinator);

  <K> Relation<K> preProcess(Relation<K> rel, SqlMessageCollector collector, TaskCoordinator coordinator);

  Tuple onResult(Tuple tuple, SqlMessageCollector collector, TaskCoordinator coordinator);

  <K> Relation<K> onResult(Relation<K> rel, SqlMessageCollector collector, TaskCoordinator coordinator);

}

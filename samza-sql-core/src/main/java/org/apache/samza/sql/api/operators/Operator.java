/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.api.operators;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;


/**
 * This class defines the common interface for operator classes, no matter what input data are.
 *
 * <p> It extends the <code>InitableTask</code> and <code>WindowableTask</code> to reuse the interface methods
 * <code>init</code> and <code>window</code> for initialization and timeout operations
 *
 */
public interface Operator {

  /**
   * Method to the specification of this <code>Operator</code>
   *
   * @return The <code>OperatorSpec</code> object that defines the configuration/parameters of the operator
   */
  OperatorSpec getSpec();

  /**
   * Method to initialize the operator
   *
   * @param config The configuration object
   * @param context The task context
   * @param userCb The user callback functions.
   * @throws Exception Throws Exception if failed to initialize the operator
   */
  void init(Config config, TaskContext context, OperatorCallback userCb) throws Exception;

  /**
   * Method to perform a relational algebra on a set of relations, or a relation-to-stream function
   *
   * <p> The actual implementation of relational logic is performed by the implementation of this method.
   * The <code>collector</code> object is used by the operator to send their output to
   *
   * @param deltaRelation The changed rows in the input relation, including the inserts/deletes/updates
   * @param collector The <code>SqlMessageCollector</code> object that accepts outputs from the operator
   * @throws Exception Throws exception if failed
   */
  <K> void process(Relation<K> deltaRelation, SqlMessageCollector collector) throws Exception;

  /**
   * Interface method to process on an input tuple.
   *
   * @param tuple The input tuple, which has the incoming message from a stream
   * @param collector The <code>SqlMessageCollector</code> object that accepts outputs from the operator
   * @throws Exception Throws exception if failed
   */
  void process(Tuple tuple, SqlMessageCollector collector) throws Exception;

  /**
   * Method to refresh the result when a timer expires
   *
   * @param timeNano The current system time in nano second
   * @throws Exception Throws exception if failed
   */
  void refresh(long timeNano, SqlMessageCollector collector, TaskCoordinator coordinator) throws Exception;

}

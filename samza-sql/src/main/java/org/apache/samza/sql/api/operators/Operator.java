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

import org.apache.samza.sql.api.operators.spec.OperatorSpec;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.task.TaskContext;


/**
 * This class defines the common interface for operator classes, no matter what input data are.
 *
 * <p>The basic methods an operator needs to support include:
 * <ul>
 * <li><code>init</code> via the <code>InitSystemContext</code>
 * <li><code>timeout</code> method triggered when timeout happened
 * <li><code>getSpec</code> that returns the specification object of the operator
 * </ul>
 *
 */
public interface Operator {
  /**
   * method to initialize the operator via the <code>InitSystemContext</code>
   *
   * @param initContext
   *     The init context object that provides interface to recover states from the task context
   * @throws Exception
   *     Throw exception if failed to initialize the store
   */
  void init(TaskContext context) throws Exception;

  /**
   * method that is to be called when timer expires
   *
   * @param currentSystemNano
   *     the current system time in nano-second
   * @param context
   *     the <code>RuntimeSystemContext</code> object that allows the operator to send their output to
   * @throws Exception
   *     Throws exception if failed
   */
  void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception;

  /**
   * method to the specification of this <code>Operator</code>
   *
   * @return
   *     The <code>OperatorSpec</code> object that defines the configuration/parameters of the operator
   */
  OperatorSpec getSpec();

}

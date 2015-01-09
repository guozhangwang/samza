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
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;


/**
 * This class defines the common interface for operator classes, no matter what input data are.
 *
 * <p>The basic methods an operator needs to support include:
 * <ul>
 * <li><code>init</code> via the <code>InitSystemContext</code>
 * <li><code>timeout</code> method triggered when timeout happened
 * <li><code>getId</code> that returns the unique ID of the operator in the task
 * </ul>
 *
 */
public interface Operator {
  /**
   * interface method to initialize the operator via the <code>RelationStore</code>
   *
   * @param initContext
   *     The init context object that provides interface to recover states from the task context
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
  public void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception;

  /**
   * interface method to get the unique ID of the operator conveniently
   *
   * @return
   *     the unique ID of the operator
   */
  public String getId();

  /**
   * Access method to the specification of this <code>RelationOperator</code>
   *
   * @return
   *     A list of <code>RelationSpec</code> that is the input of the corresponding operator
   */
  public OperatorSpec getSpec();

}

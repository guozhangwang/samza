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

package org.apache.samza.sql.operators.factory;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.task.TaskContext;


/**
 * An abstract class that encapsulate the basic information and methods that all operator classes should implement.
 *
 */
public abstract class SimpleOperator {
  /**
   * The specification of this operator
   */
  private final OperatorSpec spec;

  /**
   * Ctor of <code>SimpleOperator</code> class
   *
   * @param spec The specification of this operator
   */
  public SimpleOperator(OperatorSpec spec) {
    this.spec = spec;
  }

  public OperatorSpec getSpec() {
    return this.spec;
  }

  public abstract void init(Config config, TaskContext context) throws Exception;

}

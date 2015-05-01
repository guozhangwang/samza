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

package org.apache.samza.sql.operators.window;

import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.sql.SqlMessageCollector;


/**
 * This class defines the automated operator of a {@link org.apache.samza.sql.operators.window.FullStateTimeWindowOp}
 */
public class FullStateTimeWindowAutoOp extends FullStateTimeWindowOp implements TupleOperator {

  public FullStateTimeWindowAutoOp(WindowOpSpec spec) {
    super(spec);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(Tuple tuple, SqlMessageCollector collector) throws Exception {
    // TODO Auto-generated method stub

  }

}

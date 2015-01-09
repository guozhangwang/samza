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

package org.apache.samza.sql.store;

import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.operators.window.WindowState;
import org.apache.samza.task.TaskContext;


public class SqlContextManager implements InitSystemContext {

  private final TaskContext context;

  public SqlContextManager(TaskContext context) {
    // TODO Auto-generated constructor stub
    this.context = context;
  }

  @Override
  public Relation getRelation(String spec) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<WindowState> getWindowStates(String wndName) {
    // TODO Auto-generated method stub
    return null;
  }

}

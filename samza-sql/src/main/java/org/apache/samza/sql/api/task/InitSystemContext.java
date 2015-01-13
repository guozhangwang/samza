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

package org.apache.samza.sql.api.task;

import org.apache.samza.sql.api.data.Relation;


/**
 * This class defines interface of an operator initializing context to access various types of recoverable states for an operator,
 * including relations generated as intermediate results in the SQL query, the windowing operators' internal window states,
 * and the relations that are stored and backed in external or local databases.
 *
 */
public interface InitSystemContext {

  /**
   * get a relation based on its name from the system context
   *
   * @param relationName
   *     the unique name of the relation to be opened and returned
   * @return
   *     the relation object that is corresponding to the name
   */
  public Relation getRelation(String relationName);

}

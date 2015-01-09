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

package org.apache.samza.sql.api.data;

/**
 * This class defines the interface of <code>Tuple</code>, which is defined as a entry from the incoming stream, or one row in a <code>Relation</code>.
 *
 */
public interface Tuple {
  /**
   * Access method to get the corresponding message in the incoming/outgoing tuple
   *
   * @return
   *     message object in the tuple
   */
  Object getMessage();

  /**
   * Access method to indicate whether the tuple is a delete tuple or an insert tuple
   *
   * @return
   *     a boolean value indicates whether the current tuple is a delete or insert message
   */
  boolean isDelete();

  /**
   * Access method to a field value by name
   *
   * @param name
   *     The field name to get the value from
   * @return
   *     The field object corresponding to the name
   */
  Object getField(String name);

  /**
   * get the name of the tuple
   *
   * @return
   *     the stream name
   */
  public String getStreamName();
}

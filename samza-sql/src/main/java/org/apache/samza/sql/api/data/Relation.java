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

import java.util.Iterator;


/**
 * This class defines the interface of <code>Relation</code>, which is defined as a map of <code>Tuple</code>.
 *
 * <p>The interface defines the set of common operations on a bag of <code>Tuple</code>s that are supported,
 * including <code>get</code>, <code>put</code>, <code>delete</code>, and <code>iterator</code> that allows to iterate through all tuples.
 *
 */

public interface Relation {

  /**
   * Retrieves the tuple from the relation
   *
   * @param key
   *     The primary key for the tuple to be retrieved
   * @return
   *     The tuple corresponding to the <code>key</code>
   */
  public Tuple get(Object key);

  /**
   * Store the tuple into the relation
   *
   * @param key
   *     The primary key for the tuple to be stored
   * @param tuple
   *     The tuple to be stored in the stored relation
   * @throws Exception
   *     Throws exception when failed
   */
  public void put(Object key, Tuple tuple) throws Exception;

  /**
   * Delete the tuple from the relation
   *
   * @param key
   *     The primary key for the tuple to be stored
   * @return
   *     The tuple to be stored in the stored relation
   */
  public Tuple delete(Object key);

  /**
   * Returns an iterator of the relation
   *
   * <p>Note that some relations stored in the remote database may not be able to support this function.
   *
   * @return
   *     The tuple to be stored in the stored relation
   */
  public Iterator<Tuple> iterator();

  /**
   * Returns the specification of this relation
   *
   * @return
   *     the specification of the relation
   */
  public RelationSpec getSpec();
}

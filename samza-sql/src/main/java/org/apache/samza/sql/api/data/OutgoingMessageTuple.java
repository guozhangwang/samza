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

import org.apache.samza.system.OutgoingMessageEnvelope;


/**
 * This interface class defines a specific type of tuples that includes system output (i.e. <code>OutgoingMessageEnvelope</code>)
 *
 * <p>The separation of this interface from the general <code>Tuple</code> interface serves two purposes:
 * <ul>
 * <li>i) identify that the tuple is a system output message
 * <li>ii) provide additional access methods to information that's specific to system output, like <code>SystemStream</code> and <code>partitionKey</code>
 * </ul>
 *
 */
public interface OutgoingMessageTuple extends Tuple {

  /**
   * Get method for the <code>OutgoingMessageEnvelope</code> object associated with this tuple
   *
   * @return
   *     the <code>OutgoingMessageEnvelope</code> associated with this tuple
   */
  OutgoingMessageEnvelope getOutgoingMessageEnvelope();

}

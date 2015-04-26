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

package org.apache.samza.sql.window.storage;

/**
 * This class implements key that is based on time
 */
public class TimeKey extends WindowKey {
  private final Long timeNano;

  public TimeKey(long timeNano) {
    this.timeNano = timeNano;
  }

  @Override
  public int compareTo(WindowKey o) {
    if (!(o instanceof TimeKey)) {
      throw new IllegalArgumentException("Cannot compare TimeKey to " + o.getClass().getName());
    }

    TimeKey other = (TimeKey) o;
    return this.timeNano.compareTo(other.timeNano);
  }

  public long getTimeNano() {
    return this.timeNano;
  }

}

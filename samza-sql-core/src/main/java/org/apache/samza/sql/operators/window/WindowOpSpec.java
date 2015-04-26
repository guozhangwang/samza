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

import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.operators.factory.SimpleOperatorSpec;


/**
 * This class implements the specification class for window operators
 */
public class WindowOpSpec extends SimpleOperatorSpec implements OperatorSpec {

  public enum WindowSizeUnit {
    TIME_SEC,
    TIME_MS,
    TIME_MICRO,
    TIME_NANO,
    TUPLE
  }

  public enum WindowInitBoundary {
    HOUR,
    MIN,
    SEC,
    MS,
    MICRO,
    DEFAULT
  }

  public enum WindowType {
    FIXED_WND,
    SESSION_WND
  }

  public static class MessageStoreSpec {
    public enum StoreType {
      PREFIX_STORE,
      OFFSET_STORE,
      TIME_AND_OFFSET_STORE
    }
    public List<String> prefixFields;
    public List<String> orderKeys;
  }

  /**
   * The window size in seconds/tuples
   */
  private final int size;

  private final int stepSize;

  private final WindowSizeUnit unit;

  private final WindowType type;

  private final RetentionPolicy retention;

  private final MessageStoreSpec msgStoreSpec;

  private final String timeField;

  private final WindowInitBoundary initBoundary;
  /**
   * Default ctor of the <code>WindowSpec</code> object
   *
   * @param id The identifier of the operator
   * @param input The input stream entity
   * @param output The output relation entity
   * @param size The window size
   * @param unit The window size measuring unit
   * @param type The type of window: e.g. session window or fixed window
   * @param retention The retention policy for the window operator
   */
  public WindowOpSpec(String id, EntityName input, EntityName output, int size, int stepSize, WindowSizeUnit unit,
      WindowType type, RetentionPolicy retention, MessageStoreSpec msgStoreSpec, String timeField, WindowInitBoundary initBoundary) {
    super(id, input, output);
    this.size = size;
    this.retention = retention;
    this.unit = unit;
    this.type = type;
    this.msgStoreSpec = msgStoreSpec;
    this.timeField = timeField;
    this.stepSize = stepSize;
    this.initBoundary = initBoundary;
  }

  //TODO: stub to let test code compilation pass. Need to modify to construct real window operator spec
  public WindowOpSpec(String id, EntityName input, EntityName output, int size) {
    this(id, input, output, size, size, WindowSizeUnit.TIME_SEC, WindowType.FIXED_WND, null, null, null, null);
  }

  /**
   * Method to get the window size
   *
   * @return The window size
   */
  public int getSize() {
    return this.size;
  }

  public int getStepSize() {
    return this.stepSize;
  }

  public boolean isTimeWindow() {
    return !this.unit.equals(WindowSizeUnit.TUPLE);
  }

  public WindowSizeUnit getUnit() {
    return this.unit;
  }

  public WindowInitBoundary getInitBoundary() {
    return this.initBoundary;
  }

  public boolean isSessionWindow() {
    return this.type.equals(WindowType.SESSION_WND);
  }

  public RetentionPolicy getRetention() {
    return this.retention;
  }

  public MessageStoreSpec getMessageStoreSpec() {
    return this.msgStoreSpec;
  }

  public String getTimeField() {
    return this.timeField;
  }

  public long getNanoTime(long longValue) {
    switch (this.unit) {
      case TIME_SEC:
        return longValue * 1000000000;
      case TIME_MS:
        return longValue * 1000000;
      case TIME_MICRO:
        return longValue * 1000;
      case TIME_NANO:
      default:
        return longValue;
    }
  }
}

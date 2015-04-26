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

import java.util.concurrent.TimeUnit;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.operators.factory.SimpleOperator;
import org.apache.samza.sql.window.storage.WindowKey;
import org.apache.samza.sql.window.storage.WindowOutputStream;
import org.apache.samza.sql.window.storage.WindowState;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;


/**
 * This abstract class is the base for all window operators.
 */
public abstract class WindowOp extends SimpleOperator {

  protected final WindowOpSpec spec;
  protected final String wndId;
  protected final RetentionPolicy retention;

  protected KeyValueStore<WindowKey, WindowState> wndStore;
  protected boolean isInputDisabled = false;

  @SuppressWarnings("rawtypes")
  protected WindowOutputStream outputStream;

  protected Entry<WindowKey, WindowState> firstWnd = null;

  WindowOp(WindowOpSpec spec) {
    super(spec);
    this.spec = spec;
    this.wndId = this.spec.getId();
    this.retention = this.spec.getRetention();
  }

  @Override
  public WindowOpSpec getSpec() {
    return this.spec;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    this.wndStore = (KeyValueStore<WindowKey, WindowState>) context.getStore("wnd-store-" + this.wndId);
  }

  protected boolean isInputDisabled() {
    return this.isInputDisabled;
  }

  protected Entry<WindowKey, WindowState> getFirstWnd() {
    return this.firstWnd;
  }

  protected void resetFirstWnd() {
    KeyValueIterator<WindowKey, WindowState> wndIter = this.wndStore.all();
    while (wndIter.hasNext()) {
      Entry<WindowKey, WindowState> wnd = wndIter.next();
      if (!this.retention.isExpired(wnd.getValue())) {
        this.firstWnd = wnd;
        return;
      }
    }
    this.firstWnd = null;
  }

  protected long findInitWndStartTimeNano(long nanoMsgTime) {
    switch (this.getSpec().getInitBoundary()) {
      case HOUR:
        return TimeUnit.HOURS.toNanos(TimeUnit.NANOSECONDS.toHours(nanoMsgTime));
      case MIN:
        return TimeUnit.MINUTES.toNanos(TimeUnit.NANOSECONDS.toMinutes(nanoMsgTime));
      case SEC:
        return nanoMsgTime / 1000000000 * 1000000000;
      case MS:
        return nanoMsgTime / 1000000 * 1000000;
      case MICRO:
        return nanoMsgTime / 1000 * 1000;
      default:
        return nanoMsgTime;
    }
  }

  abstract public void addMessage(Tuple tuple) throws Exception;

  @SuppressWarnings("rawtypes")
  abstract public WindowOutputStream getResult();

  abstract public void flush() throws Exception;

  protected abstract boolean isRepeatedMessage(Tuple msg);

  protected abstract void purge();
}

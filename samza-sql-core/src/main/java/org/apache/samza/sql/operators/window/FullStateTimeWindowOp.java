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

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.exception.OperatorException;
import org.apache.samza.sql.window.storage.Range;
import org.apache.samza.sql.window.storage.TimeAndOffsetKey;
import org.apache.samza.sql.window.storage.TimeKey;
import org.apache.samza.sql.window.storage.WindowKey;
import org.apache.samza.sql.window.storage.WindowOutputStream;
import org.apache.samza.sql.window.storage.WindowState;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.sql.LongOffset;
import org.apache.samza.task.TaskContext;


/**
 * This class implements a full state time-based window operator.
 */
public class FullStateTimeWindowOp extends FullStateWindowOp implements FullStateTimeWindow {

  private final String outputStreamName;

  public FullStateTimeWindowOp(WindowOpSpec spec) {
    super(spec);
    this.outputStreamName = String.format("wnd-outstrm-%s", wndId);
  }

  //TODO: stub to be updated w/ real window spec constructions
  public FullStateTimeWindowOp(String wndId, int size, String inputStrm, String outputEntity) {
    // TODO Auto-generated constructor stub
    super(null);
    this.outputStreamName = String.format("wnd-outstrm-%s", wndId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    super.init(config, context);
    this.outputStream =
        new WindowOutputStream<WindowKey>((Stream<WindowKey>) context.getStore(this.outputStreamName),
            EntityName.getStreamName(this.outputStreamName), this.getSpec().getMessageStoreSpec().orderKeys);
  }

  @Override
  public void addMessage(Tuple tuple) throws Exception {
    //    1. Check whether the message selector is disabled for this window operator
    //    1. If yes, log a warning and throws exception
    if (this.isInputDisabled()) {
      throw OperatorException.getInputDisabledException(String.format(
          "The input to this window operator is disabled. Operator ID: %s", this.getSpec().getId()));
    }
    // 2. Check whether the message is out-of-retention
    //    1. If yes, send the message to error topic and return
    if (this.spec.getRetention().isExpired(tuple)) {
      throw OperatorException.getMessageTooOldException(String.format("Message is out-of-retention. Operator ID: %s",
          this.getSpec().getId()));
    }
    // 3. Check to see whether the message is a replay that we have already sent result for
    //    1. If yes, return as NOOP
    //    2. Otherwise, continue
    if (this.isRepeatedMessage(tuple)) {
      return;
    }
    // 4. Check to see whether the incoming message belongs to a new window
    //    1. If yes, Call openNextWindow() and repeat
    //    2. Otherwise, continue
    List<WindowKey> allWnds = this.findAllWindows(tuple);
    // 5. Add incoming message to all windows that includes it and add those windows to pending updates list
    // 6. For all windows in pending updates list, do the following
    //    1. updateResult() to refresh the aggregation results
    //    2. Check to see whether we need to emit the corresponding window outputs
    //       1. If the window is the current window, check with early emission policy to see whether we need to flush aggregated result
    //          1. If yes, add this window to pending flush list and continue
    //       2. For any window, check the full size policy to see whether we need to send out the window output
    //          1. If yes, add this window to pending flush list and continue
    //       3. For a past window, check with late arrival policy to see whether we need to send out past window outputs
    //          1. If yes, add this window to pending flush list and continue
    for (WindowKey key : allWnds) {
      WindowState wnd = this.wndStore.get(key);
      if (this.getMessageTimeNano(tuple) >= wnd.getStartTimeNano()
          && this.getMessageTimeNano(tuple) <= wnd.getEndTimeNano()) {
        addMessageToWindow(tuple, key, wnd);
      }
    }
    // 7. Send out pending windows updates
    //    1. For each window in pending flush list, send the updates to the next operator
    //       1. If window outputs can't be accepted, disable the message selector from delivering messages to this window operator and return
    //    2. Finally, flush the Window State Store and Window Mark
    //       1. If retention limit is triggered, purge the out-of-retention windows/messages from Message Store and Window Meta Store before flushing
    //
    // Helper function openNextWindow()
    //
    // 1. If there is any new messages in **current window**, add **current window** to pending updates list
    // 2. Create the next window and add all **in-time** messages to the next window
    // 3. set next window to **current window**, return
  }

  private void addMessageToWindow(Tuple tuple, WindowKey key, WindowState wnd) {
    // 6. For all windows in pending updates list, do the following
    //    1. updateResult() to refresh the aggregation results
    updateWindow(tuple, key, wnd);
    //    2. Check to see whether we need to emit the corresponding window outputs
    //       1. If the window is the current window, check with early emission policy to see whether we need to flush aggregated result
    //          1. If yes, add this window to pending flush list and continue
    //       2. For any window, check the full size policy to see whether we need to send out the window output
    //          1. If yes, add this window to pending flush list and continue
    //       3. For a past window, check with late arrival policy to see whether we need to send out past window outputs
    //          1. If yes, add this window to pending flush list and continue
    if (isScheduledOutput(key) || isFullSizeOutput(key) || isLateArrivalOutput(key)) {
      addToOutputs(tuple, key);
    }
  }

  private boolean isLateArrivalOutput(WindowKey key) {
    // TODO: add implementation of late arrival policy checks here
    return false;
  }

  private boolean isFullSizeOutput(WindowKey key) {
    // TODO: add implementation of full size output policy checks here
    return false;
  }

  private boolean isScheduledOutput(WindowKey key) {
    // TODO: add implementation of scheduled output policy checks here
    return false;
  }

  @SuppressWarnings("unchecked")
  private void addToOutputs(Tuple tuple, WindowKey key) {
    ((WindowOutputStream<WindowKey>) this.outputStream).put(
        new TimeAndOffsetKey(tuple.getMessageTimeNano(), tuple.getOffset()), tuple);
  }

  private WindowKey getMessageStoreKey(Tuple tuple) {
    return this.messageStore.getKey(new TimeAndOffsetKey(tuple.getMessageTimeNano(), tuple.getOffset()), tuple);
  }

  private void updateWindow(Tuple tuple, WindowKey key, WindowState wnd) {
    // For full state window, no need to update the aggregated value
    wnd.setLastOffset(tuple.getOffset());
    this.wndStore.put(key, wnd);
  }

  private List<WindowKey> findAllWindows(Tuple tuple) {
    List<WindowKey> wndKeys = new ArrayList<WindowKey>();
    long msgTime = this.getMessageTimeNano(tuple);
    Entry<WindowKey, WindowState> firstWnd = getFirstWnd();
    // assuming this is a fixed size window, the windows that include the msgTime will be computed based on the offset in time
    // from the firstWnd and the corresponding widow size
    long stepSize = this.getStepSizeNano();
    long size = this.getSizeNano();
    long startWnd = 0;
    if (firstWnd == null) {
      startWnd = this.findInitWndStartTimeNano(msgTime);
    } else {
      long offsetTime = msgTime - size - firstWnd.getValue().getStartTimeNano();
      long numOfWnds = offsetTime / stepSize;
      if (offsetTime < 0) {
        numOfWnds--;
      }
      startWnd = firstWnd.getValue().getStartTimeNano() + numOfWnds * stepSize;
    }
    while (startWnd < msgTime) {
      TimeKey wndKey = new TimeKey(startWnd);
      if (this.wndStore.get(wndKey) == null) {
        // If the window if missing, open the new window and save to the store
        long startTime = wndKey.getTimeNano();
        long endTime = startTime + this.getStepSizeNano();
        this.wndStore.put(wndKey, new WindowState(tuple.getOffset(), startTime, endTime));
      }
      wndKeys.add(wndKey);
      startWnd += stepSize;
    }
    return wndKeys;
  }

  private long getSizeNano() {
    return this.getSpec().getNanoTime(this.getSpec().getSize());
  }

  private long getStepSizeNano() {
    return this.getSpec().getNanoTime(this.getSpec().getStepSize());
  }

  private long getMessageTimeNano(Tuple tuple) {
    if (this.getSpec().getTimeField() != null) {
      return this.getSpec().getNanoTime(tuple.getMessage().getFieldData(this.getSpec().getTimeField()).longValue());
    }
    // TODO: need to get the event time in broker envelope
    return tuple.getMessageTimeNano();
  }

  @SuppressWarnings("unchecked")
  @Override
  public WindowOutputStream<WindowKey> getResult() {
    return this.outputStream;
  }

  @Override
  public void flush() throws Exception {
    // clear the current window output, purge the out-of-retention windows,
    // and flush the wndStore and messageStore
    this.outputStream.clear();
    this.purge();
    this.wndStore.flush();
    this.messageStore.flush();
  }

  private Range<WindowKey> getMessageKeyRangeByTime(Range<Long> timeRange) {
    // TODO: the range may need to be adjusted to reflect retention policies
    WindowKey minKey = new TimeAndOffsetKey(timeRange.getMin(), LongOffset.getMinOffset());
    // Set to the next nanosecond w/ minimum offset since the right boundary is exclusive
    // TODO: need to work on Offset that is not LongOffset
    WindowKey maxKey = new TimeAndOffsetKey(timeRange.getMax() + 1, LongOffset.getMinOffset());
    return Range.between(minKey, maxKey);
  }

  @Override
  public KeyValueIterator<WindowKey, Tuple> getMessages(Range<Long> timeRange,
      List<Entry<String, Object>> filterFields) {
    Range<WindowKey> keyRange = getMessageKeyRangeByTime(timeRange);
    // This is to get the range from the messageStore and possibly apply filters in the key or the value
    return this.messageStore.getMessages(keyRange, filterFields);
  }

  @Override
  public KeyValueIterator<WindowKey, Tuple> getMessages(Range<Long> timeRange) {
    // TODO Auto-generated method stub
    return this.getMessages(timeRange, new ArrayList<Entry<String, Object>>());
  }

  @Override
  protected boolean isRepeatedMessage(Tuple msg) {
    // TODO: this needs to check the message store to see whether the message is already there.
    return this.messageStore.get(this.getMessageStoreKey(msg)) != null;
  }

  @Override
  protected void purge() {
    // check all wnds in the wndStore and all messages in the messageStore to see whether we need to purge/remove out-of-retention windows
    KeyValueIterator<WindowKey, WindowState> iter = this.wndStore.all();
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    while (iter.hasNext()) {
      Entry<WindowKey, WindowState> entry = iter.next();
      if (this.retention.isExpired(entry.getValue())) {
        this.wndStore.delete(entry.getKey());
        if (maxTime < entry.getValue().getEndTimeNano()) {
          maxTime = entry.getValue().getEndTimeNano();
        }
        if (minTime > entry.getValue().getStartTimeNano()) {
          minTime = entry.getValue().getStartTimeNano();
        }
      }
    }

    // now purge message store
    // Set to the next nanosecond w/ minimum offset since the right boundary is exclusive
    Range<WindowKey> range =
        Range.between(new TimeAndOffsetKey(minTime, LongOffset.getMinOffset()), new TimeAndOffsetKey(maxTime + 1,
            LongOffset.getMinOffset()));
    this.messageStore.purge(range);
  }

}
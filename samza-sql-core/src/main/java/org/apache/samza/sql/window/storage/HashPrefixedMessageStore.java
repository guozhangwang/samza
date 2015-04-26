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

import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Stream;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;


/**
 * This class implements a {@link org.apache.samza.sql.window.storage.MessageStore} that uses field values as prefix
 */
public class HashPrefixedMessageStore extends MessageStore {

  private final char PREFIX_DENOMINATOR = '_';

  public HashPrefixedMessageStore(Stream<WindowKey> msgStore, List<String> orderKeyFields, EntityName strmName) {
    super(msgStore, orderKeyFields, strmName);
  }

  private String getPrefix(List<Entry<String, Object>> filterFields) {
    // TODO: need to validate that the filterFields are exactly the fields listed in orderKeyFields
    StringBuffer strBuffer = new StringBuffer();
    for (Entry<String, Object> entry : filterFields) {
      strBuffer.append(entry.getValue().toString()).append(PREFIX_DENOMINATOR);
    }
    return strBuffer.toString();
  }

  private Range<WindowKey> getPrefixedRange(Range<WindowKey> range, List<Entry<String, Object>> filterFields) {
    String prefixStr = this.getPrefix(filterFields);
    return Range.between(new PrefixedKey(prefixStr, range.getMin()), new PrefixedKey(prefixStr, range.getMax()));
  }

  @Override
  public WindowKey getKey(WindowKey extKey, Tuple tuple) {
    StringBuilder prefixStr = new StringBuilder();
    for (String field : getOrderKeys()) {
      prefixStr.append(tuple.getMessage().getFieldData(field).toString()).append(PREFIX_DENOMINATOR);
    }
    return new PrefixedKey(prefixStr.toString(), extKey);
  }

  @Override
  public KeyValueIterator<WindowKey, Tuple> getMessages(Range<WindowKey> extRange,
      List<Entry<String, Object>> filterFields) {
    Range<WindowKey> prefixRange = this.getPrefixedRange(extRange, filterFields);
    return this.range(prefixRange.getMin(), prefixRange.getMax());
  }

  @Override
  public void purge(Range<WindowKey> extRange) {
    // Naive implementation of purge for now, will be costly since it traverses through all possible prefix keys
    KeyValueIterator<WindowKey, Tuple> iter = this.all();
    while (iter.hasNext()) {
      Entry<WindowKey, Tuple> entry = iter.next();
      PrefixedKey key = (PrefixedKey) entry.getKey();
      if (extRange.contains(key.getKey()) || extRange.getMin().compareTo(key.getKey()) > 0) {
        this.delete(entry.getKey());
      }
    }
  }
}

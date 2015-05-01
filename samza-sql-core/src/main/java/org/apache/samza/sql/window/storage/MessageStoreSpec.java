package org.apache.samza.sql.window.storage;

import java.util.ArrayList;
import java.util.List;


public class MessageStoreSpec {
  public enum StoreType {
    PREFIX_STORE,
    OFFSET_STORE,
    TIME_AND_OFFSET_STORE
  }

  private final StoreType type;
  private final List<String> prefixFields;
  private final String timestampField;
  private final List<String> fullOrderFields = new ArrayList<String>();

  public MessageStoreSpec(StoreType type, List<String> prefixFields, String timestampField) {
    this.type = type;
    this.prefixFields = prefixFields;
    this.timestampField = timestampField;
    if (prefixFields != null && !prefixFields.isEmpty()) {
      this.fullOrderFields.addAll(prefixFields);
    }
    if (timestampField != null && !timestampField.isEmpty()) {
      this.fullOrderFields.add(timestampField);
    }

  }

  public List<String> getOrderFields() {
    return this.fullOrderFields;
  }

  public List<String> getPrefixFields() {
    return this.prefixFields;
  }

  public String getTimestampField() {
    return this.timestampField;
  }

  public StoreType getType() {
    return this.type;
  }
}

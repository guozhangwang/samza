package org.apache.samza.sql.api.data;

import java.util.Map;


public interface Schema {

  enum Type {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BOOLEAN,
    STRING,
    BYTES,
    STRUCT,
    ARRAY,
    MAP
  };

  Type getType();

  Schema getElementType();

  Schema getValueType();

  Map<String, Schema> getFields();

  Schema getFieldType(String fldName);

  Data read(Object object);

  Data transform(Data inputData);

  boolean equals(Schema other);
}

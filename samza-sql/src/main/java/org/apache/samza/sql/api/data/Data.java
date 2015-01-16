package org.apache.samza.sql.api.data;

import java.util.List;
import java.util.Map;


public interface Data {

  Schema schema();

  Object value();

  int intValue();

  long longValue();

  float floatValue();

  double doubleValue();

  boolean booleanValue();

  String strValue();

  byte[] bytesValue();

  List<Object> arrayValue();

  Map<Object, Object> mapValue();

  Data getElement(int index);

  Data getFieldData(String fldName);

}

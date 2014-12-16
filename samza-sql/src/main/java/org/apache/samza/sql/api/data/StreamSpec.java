package org.apache.samza.sql.api.data;

public interface StreamSpec {
  /**
   * get the name of the tuple
   *
   * @return
   *     the tuple name
   */
  public String getName();
}

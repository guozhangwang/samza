package org.apache.samza.sql.api.data;

/**
 * This class defines the interface of <code>Tuple</code>, which is defined as a entry from the incoming stream, or one row in a <code>Relation</code>.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface Tuple {
  /**
   * Access method to get the corresponding message in the incoming/outgoing tuple
   *
   * @return
   *     message object in the tuple
   */
  Object getMessage();

  /**
   * Access method to indicate whether the tuple is a delete tuple or an insert tuple
   *
   * @return
   *     a boolean value indicates whether the current tuple is a delete or insert message
   */
  boolean isDelete();

  /**
   * Access method to a field value by name
   *
   * @param name
   *     The field name to get the value from
   * @return
   *     The field object corresponding to the name
   */
  Object getField(String name);

  /**
   * Access method to get the specification of the stream that the tuple is from
   *
   * @return
   *     Specification of the stream
   */
  StreamSpec getStreamSpec();
}

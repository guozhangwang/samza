package org.apache.samza.sql.api.data;

/**
 * This interface defines the APIs for all specification to create a relational table.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface RelationSpec {
  /**
   * get the primary key field name defined for this table
   *
   * @return
   *     the name of the primary key field
   */
  public String getPrimaryKey();

  /**
   * get the name of the relation created by CREATE TABLE
   *
   * @return
   *     the relation name
   */
  public String getName();
}

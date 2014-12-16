package org.apache.samza.sql.api.operators.spec;

import org.apache.samza.sql.api.data.RelationSpec;


/**
 * This interface defines the APIs for specification of <code>RelationRelationOperator</code>, including join, select, where, group-by, etc.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface RelationRelationSpec extends RelationOperatorSpec {
  /**
   * Get the specification of the output relation of the <code>RelationRelationOperator</code>
   *
   * @return
   *     The specification object of the output relation
   */
  public RelationSpec getOutputSpec();
}

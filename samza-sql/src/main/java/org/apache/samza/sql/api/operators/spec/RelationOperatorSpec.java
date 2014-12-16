package org.apache.samza.sql.api.operators.spec;

import java.util.List;

import org.apache.samza.sql.api.data.RelationSpec;


/**
 * This class defines a generic interface for specification of a <code>RelationOperator</code> that takes <code>Relation</code> as input
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface RelationOperatorSpec extends OperatorSpec {
  /**
   * Access method to the list of input relation specifications
   *
   * @return
   *     A list of <code>RelationSpec</code> that is the input of the corresponding operator
   */
  public List<RelationSpec> getInputSpecs();

}

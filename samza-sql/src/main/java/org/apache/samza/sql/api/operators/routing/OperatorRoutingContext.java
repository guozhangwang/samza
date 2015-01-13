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

package org.apache.samza.sql.api.operators.routing;

import java.util.Iterator;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;


/**
 * This interface class defines interface methods to connect operators together.
 *
 * <p>The methods included in this interface class allow a user to
 * <ul>
 * <li>i)   set the input operators
 * <li>ii)  connect the operators together
 * <li>iii) get the next operators and system input operators
 * <li>iv)  iterate through each and every operator connected in the routing context
 * </ul>
 *
 */
public interface OperatorRoutingContext {

  /**
   * This method adds a <code>TupleOperator</code> as one of the input operators.
   *
   * @param inputOp
   *     The <code>TupleOperator</code> that takes the input tuples directly.
   * @throws Exception
   *     Throws exception if failed
   */
  public void setSystemInputOperator(TupleOperator inputOp) throws Exception;

  /**
   * This method adds a <code>RelationOperator</code> as one of the input operators
   *
   * @param inputOp
   *     The <code>RelationOperator</code> that takes the input relations directly
   * @throws Exception
   *     Throws exception if failed
   */
  public void setSystemInputOperator(RelationOperator inputOp) throws Exception;

  /**
   * This method returns all operators that directly takes input variables instead of connected to the output from other operators.
   *
   * <p>The returned <code>MultiValueMap</code> is keyed by the input <code>Tuple</code>'s stream name or <code>Relation</code> name.
   *
   * @return
   *     A <code>MultiValueMap</code> that uses the input <code>Tuple</code>'s stream name or <code>Relation</code> name as the key,
   *     and a collection of operators as the value.
   */
  public MultiValueMap getSystemInputOps();

  /**
   * This method sets the next <code>RelationOperator</code> that should take the output of the current operator.
   *
   * @param currentOpId
   *     The identifier of the current operator.
   * @param nextOp
   *     The <code>RelationOperator</code> to take the output relation from the current operator
   * @throws Exception
   *     Throws exception if failed.
   */
  public void setNextRelationOperator(String currentOpId, RelationOperator nextOp) throws Exception;

  /**
   * This method sets the next <code>TupleOperator</code> that should take the output of the current operator
   *
   * @param currentOpId
   *     The identifier of the current operator
   * @param nextOp
   *     The <code>TupleOperator</code> to take the output tuples from the current operator
   * @throws Exception
   *     Throws exception if failed
   */
  public void setNextTupleOperator(String currentOpId, TupleOperator nextOp) throws Exception;

  /**
   * This method gets the next <code>RelationOperator</code> connected to the current operator
   *
   * @param currentOpId
   *     The identifier of the current operator
   * @return
   *     The <code>RelationOperator</code> connected to the current operator
   */
  public RelationOperator getNextRelationOperator(String currentOpId);

  /**
   * This method gets the next <code>TupleOperator</code> connected to the current operator
   *
   * @param currentOpId
   *     The identifier of the current operator
   * @return
   *     The <code>TupleOperator</code> connected to the current operator
   */
  public TupleOperator getNextTupleOperator(String currentOpId);

  /**
   * This method gets the next operator to be triggered in terms of a timeout event
   *
   * @param currentOpId
   *     The identifier of the current operator
   * @return
   *     The next operator connected to the current one
   */
  public Operator getNextTimeoutOperator(String currentOpId);

  /**
   * This method provides an iterator to go through all operators connected via <code>OperatorRoutingContext</code>
   *
   * @return
   *     An <code>Iterator</code> for all operators connected in the routing context
   */
  public Iterator<Operator> iterator();

}

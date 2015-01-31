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

package org.apache.samza.sql.api.router;

import java.util.Iterator;
import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.TupleOperator;


/**
 * This interface class defines interface methods to connect operators together.
 *
 * <p>The <code>OperatorRouter</code> allows the user to attach operators to a relation or a stream entity,
 * if the corresponding relation/stream is included as inputs to the operator. Each operator then executes its own logic
 * and determines which relation/stream to emit the output to. Through the <code>OperatorRouter</code>, the next
 * operators attached to the corresponding output entities (i.e. relations/streams) can then be invoked to continue the
 * stream process task.
 *
 * <p>The <code>OperatorRouter</code> also allows the user to set the system input entities (i.e. relations/streams)
 * that are fed into the operators by the system outside the <code>OperatorRouter</code>, not generated by some
 * operators in the <code>OperatorRouter</code>.
 *
 * <p>The methods included in this interface class allow a user to
 * <ul>
 * <li>i)   add operators to an <code>EntityName</code>
 * <li>ii)  get the next operators attached to an <code>EntityName</code>
 * <li>iii) add and get the system input <code>EntityName</code>s
 * <li>iv)  iterate through each and every operator connected in the routing context
 * </ul>
 *
 */
public interface OperatorRouter {

  /**
   * This method adds a <code>TupleOperator</code> as one of the input operators.
   *
   * @param stream The output stream entity name
   * @param nextOp The <code>TupleOperator</code> that takes the tuples in the <code>stream</code> as an input.
   * @throws Exception Throws exception if failed
   */
  void addTupleOperator(EntityName stream, TupleOperator nextOp) throws Exception;

  /**
   * This method adds a <code>RelationOperator</code> as one of the input operators

   * @param relation The input relation entity name
   * @param nextOp The <code>RelationOperator</code> that takes the <code>relation</code> as an input
   * @throws Exception Throws exception if failed
   */
  void addRelationOperator(EntityName relation, RelationOperator nextOp) throws Exception;

  /**
   * This method gets the list of <code>RelationOperator</code>s attached to the <code>relation</code>
   *
   * @param relation The identifier of the relation entity
   * @return The list of <code>RelationOperator</code> taking <code>relation</code> as an input variable
   */
  List<RelationOperator> getRelationOperators(EntityName relation);

  /**
   * This method gets the list of <code>TupleOperator</code>s attached to the <code>stream</code>
   *
   * @param stream The identifier of the stream entity
   * @return The list of <code>TupleOperator</code> taking <code>stream</code> as an input variable
   */
  List<TupleOperator> getTupleOperators(EntityName stream);

  /**
   * This method gets the list of <code>Operator</code>s attached to an output entity (of any type)
   *
   * @param output The identifier of the output entity
   * @return The list of <code>Operator</code> taking <code>output</code> as input variables
   */
  List<Operator> getNextOperators(EntityName output);

  /**
   * This method provides an iterator to go through all operators connected via <code>OperatorRouter</code>
   *
   * @return An <code>Iterator</code> for all operators connected in the routing context
   */
  Iterator<Operator> iterator();

  /**
   * This method checks to see whether there is any <code>Operator</code> attached to the entity <code>output</code>
   *
   * @param output The output entity name
   * @return True if there is some operator attached to the <code>output</code>; false otherwise
   */
  boolean hasNextOperators(EntityName output);

  /**
   * This method adds an entity as the system input
   *
   * @param input The entity name for the system input
   */
  void addSystemInput(EntityName input);

  /**
   * This method returns the list of entities as system inputs
   *
   * @return The list of <code>EntityName</code>s as system inputs
   */
  List<EntityName> getSystemInputs();

}

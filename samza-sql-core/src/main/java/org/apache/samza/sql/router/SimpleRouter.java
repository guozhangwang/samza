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

package org.apache.samza.sql.router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.router.OperatorRouter;


/**
 * Example implementation of <code>OperatorRouter</code>
 *
 */
public class SimpleRouter implements OperatorRouter {
  /**
   * List of operators added to the <code>OperatorRouter</code>
   */
  private List<Operator> operators = new ArrayList<Operator>();

  @SuppressWarnings("rawtypes")
  /**
   * Map of <code>EntityName</code> to the list of operators associated with it
   */
  private Map<EntityName, List> nextOps = new HashMap<EntityName, List>();

  /**
   * Set of <code>EntityName</code> as system inputs
   */
  private Set<EntityName> inputEntities = new HashSet<EntityName>();

  /**
   * Set of entities that are not input entities to this OperatorRouter
   */
  private Set<EntityName> outputEntities = new HashSet<EntityName>();

  @Override
  @SuppressWarnings("unchecked")
  public void addOperator(EntityName input, Operator nextOp) {
    if (nextOps.get(input) == null) {
      nextOps.put(input, new ArrayList<Operator>());
    }
    nextOps.get(input).add(nextOp);
    operators.add(nextOp);
    // get the operator spec
    for (EntityName output : nextOp.getSpec().getOutputNames()) {
      if (inputEntities.contains(output)) {
        inputEntities.remove(output);
      }
      outputEntities.add(output);
    }
    if (!outputEntities.contains(input)) {
      inputEntities.add(input);
    }
  }

  @Override
  public Iterator<Operator> iterator() {
    return operators.iterator();
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Operator> getNextOperators(EntityName entity) {
    return nextOps.get(entity);
  }

  @Override
  public boolean hasNextOperators(EntityName output) {
    return nextOps.get(output) != null && !nextOps.get(output).isEmpty();
  }

  @Override
  public Set<EntityName> getInputEntities() {
    return this.inputEntities;
  }
}

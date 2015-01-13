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

package org.apache.samza.sql.operators.window;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.task.InitSystemContext;
import org.apache.samza.sql.api.task.RuntimeSystemContext;
import org.apache.samza.sql.operators.factory.SimpleOperator;


/**
 * This class defines an example build-in operator for a fixed size window operator that converts a stream to a relation
 *
 */
public class BoundedTimeWindow extends SimpleOperator implements TupleOperator {

  /**
   * The specification of this window operator
   */
  private final WindowSpec spec;

  /**
   * The relation that the window operator keeps internally
   */
  private Relation relation = null;

  /**
   * The list of window states of all active windows the window operator keeps in track
   */
  private List<WindowState> windowStates = null;

  /**
   * ctor that takes <code>WindowSpec</code> specification as input argument
   *
   * <p>This version of constructor is often used in an implementation of <code>SqlOperatorFactory</code>
   *
   * @param spec
   *     The window specification object
   */
  public BoundedTimeWindow(WindowSpec spec) {
    super(spec);
    this.spec = spec;
  }

  /**
   * A simplified version of ctor that allows users to randomly created a window operator w/o spec object
   *
   * @param wndId
   *     The identifier of this window operator
   * @param lengthSec
   *     The window size in seconds
   * @param input
   *     The input stream name
   * @param output
   *     The output relation name
   */
  public BoundedTimeWindow(String wndId, int lengthSec, String input, String output) {
    super(new WindowSpec(wndId, input, output, lengthSec));
    this.spec = (WindowSpec) super.getSpec();
  }

  @Override
  public void process(Tuple tuple, RuntimeSystemContext context) throws Exception {
    // for each tuple, this will evaluate the incoming tuple and update the window states.
    // If the window states allow generating output, calculate the delta changes in
    // the window relation and execute the relation operation <code>nextOp</code>
    updateWindow(tuple);
    processWindowChanges(context);
  }

  private void processWindowChanges(RuntimeSystemContext context) throws Exception {
    if (windowStateChange()) {
      context.sendToNextRelationOperator(this.spec.getId(), getWindowChanges());
    }
  }

  private Relation getWindowChanges() {
    // TODO Auto-generated method stub
    return null;
  }

  private boolean windowStateChange() {
    // TODO Auto-generated method stub
    return getWindowChanges() != null;
  }

  private void updateWindow(Tuple tuple) {
    // TODO Auto-generated method stub
    // The window states are updated here
    // And the correpsonding deltaChanges is also calculated here.
  }

  @Override
  public void timeout(long currentSystemNano, RuntimeSystemContext context) throws Exception {
    updateWindowTimeout();
    processWindowChanges(context);
    context.sendToNextTimeoutOperator(this.spec.getId(), currentSystemNano);
  }

  private void updateWindowTimeout() {
    // TODO Auto-generated method stub
    // The window states are updated here
    // And the correpsonding deltaChanges is also calculated here.
  }

  @Override
  public void init(InitSystemContext initContext) throws Exception {
    // TODO Auto-generated method stub
    if (this.relation == null) {
      this.relation = initContext.getRelation(this.spec.getOutputName());
      Relation wndStates = initContext.getRelation(this.spec.getWndStatesName());
      this.windowStates = new ArrayList<WindowState>();
      for (Iterator<Tuple> iter = wndStates.iterator(); iter.hasNext();) {
        this.windowStates.add((WindowState) iter.next().getField("WindowState"));
      }
    }
  }
}

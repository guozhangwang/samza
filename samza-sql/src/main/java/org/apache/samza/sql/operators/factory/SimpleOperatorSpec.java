package org.apache.samza.sql.operators.factory;

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.sql.api.operators.spec.OperatorSpec;


/**
 * An abstract class that encapsulate the basis information and methods all specification of operators should implement.
 *
 */
public abstract class SimpleOperatorSpec implements OperatorSpec {
  /**
   * The identifier of the corresponding operator
   */
  private final String id;

  /**
   * The list of input entity names of the corresponding operator
   */
  private final List<String> inputs = new ArrayList<String>();

  /**
   * The output entity name of the corresponding operator
   */
  private final String output;

  public SimpleOperatorSpec(String id, String input, String output) {
    this.id = id;
    this.inputs.add(input);
    this.output = output;
  }

  public SimpleOperatorSpec(String id, List<String> inputs, String output) {
    this.id = id;
    this.inputs.addAll(inputs);
    this.output = output;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public List<String> getInputNames() {
    return this.inputs;
  }

  @Override
  public String getOutputName() {
    return this.output;
  }

}

package org.apache.samza.sql.operators.factory;

import org.apache.samza.sql.api.operators.RelationOperator;
import org.apache.samza.sql.api.operators.SqlOperatorFactory;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.api.operators.spec.RelationOperatorSpec;
import org.apache.samza.sql.api.operators.spec.TupleOperatorSpec;
import org.apache.samza.sql.operators.output.SystemStreamOp;
import org.apache.samza.sql.operators.output.SystemStreamSpec;
import org.apache.samza.sql.operators.partition.PartitionOp;
import org.apache.samza.sql.operators.partition.PartitionSpec;
import org.apache.samza.sql.operators.relation.Join;
import org.apache.samza.sql.operators.relation.JoinSpec;
import org.apache.samza.sql.operators.stream.InsertStream;
import org.apache.samza.sql.operators.stream.InsertStreamSpec;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.sql.operators.window.WindowSpec;


/**
 * This simple factory class provides method to create the build-in operators per operator specification.
 * It can be extended when the build-in operators expand.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public class SimpleOperatorFactoryImpl implements SqlOperatorFactory {

  @Override
  public RelationOperator getRelationOperator(RelationOperatorSpec spec) {
    if (spec instanceof JoinSpec) {
      return new Join((JoinSpec) spec);
    } else if (spec instanceof InsertStreamSpec) {
      return new InsertStream((InsertStreamSpec) spec);
    }
    throw new UnsupportedOperationException("Unsupported operator specified: " + spec.getClass().getCanonicalName());
  }

  @Override
  public TupleOperator getTupleOperator(TupleOperatorSpec spec) {
    // TODO Auto-generated method stub
    if (spec instanceof SystemStreamSpec) {
      return new SystemStreamOp((SystemStreamSpec) spec);
    } else if (spec instanceof WindowSpec) {
      return new BoundedTimeWindow((WindowSpec) spec);
    } else if (spec instanceof PartitionSpec) {
      return new PartitionOp((PartitionSpec) spec);
    }
    throw new UnsupportedOperationException("Unsupported operator specified" + spec.getClass().getCanonicalName());
  }

}

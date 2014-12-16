package org.apache.samza.sql.task;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MultiMap;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.sql.api.operators.Operator;
import org.apache.samza.sql.api.operators.TupleOperator;
import org.apache.samza.sql.data.IncomingMessageTuple;
import org.apache.samza.sql.operators.factory.SimpleOperatorFactoryImpl;
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
import org.apache.samza.sql.store.SQLRelationStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.SqlTaskContext;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


public class StreamSqlTask implements StreamTask, InitableTask, WindowableTask {

  private SQLRelationStore store;

  private Map<String, Operator> operators = new HashMap<String, Operator>();

  private MultiMap inputOps;

  private SqlTaskContext sqlContext;

  @SuppressWarnings("unchecked")
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub
    this.sqlContext.setup(collector, coordinator);

    IncomingMessageTuple ituple = new IncomingMessageTuple(envelope);
    for (TupleOperator inputOp : (Collection<TupleOperator>) inputOps.get(ituple.getStreamSpec())) {
      inputOp.process(ituple);
    }

    this.sqlContext.reset();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
    this.sqlContext.setup(collector, coordinator);

    long currNano = System.nanoTime();
    for (TupleOperator inputOp : (Collection<TupleOperator>) inputOps.values()) {
      inputOp.timeout(currNano);
    }

    this.sqlContext.reset();
  }

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
    this.sqlContext = new SqlTaskContext() {

      private MessageCollector collector = null;
      private TaskCoordinator coordinator = null;

      @Override
      public MetricsRegistry getMetricsRegistry() {
        // TODO Auto-generated method stub
        return context.getMetricsRegistry();
      }

      @Override
      public Set<SystemStreamPartition> getSystemStreamPartitions() {
        // TODO Auto-generated method stub
        return context.getSystemStreamPartitions();
      }

      @Override
      public Object getStore(String name) {
        // TODO Auto-generated method stub
        return context.getStore(name);
      }

      @Override
      public TaskName getTaskName() {
        // TODO Auto-generated method stub
        return context.getTaskName();
      }

      @Override
      public void setup(MessageCollector collector, TaskCoordinator coordinator) {
        // TODO Auto-generated method stub
        this.collector = collector;
        this.coordinator = coordinator;
      }

      @Override
      public MessageCollector getMessageCollector() {
        // TODO Auto-generated method stub
        return this.collector;
      }

      @Override
      public TaskCoordinator getCoordinator() {
        // TODO Auto-generated method stub
        return this.coordinator;
      }

      @Override
      public void reset() {
        // TODO Auto-generated method stub
        this.collector = null;
        this.coordinator = null;
      }

    };

    SimpleOperatorFactoryImpl operatorFactory = new SimpleOperatorFactoryImpl();
    WindowSpec spec1 = new WindowSpec();
    WindowSpec spec2 = new WindowSpec();
    BoundedTimeWindow wnd1 = (BoundedTimeWindow) operatorFactory.getTupleRelationOperator(spec1);
    BoundedTimeWindow wnd2 = (BoundedTimeWindow) operatorFactory.getTupleRelationOperator(spec2);
    operators.put(spec1.getId(), wnd1);
    operators.put(spec2.getId(), wnd2);

    inputOps.put(spec1.getInputSpec(), wnd1);
    inputOps.put(spec2.getInputSpec(), wnd2);
    JoinSpec joinSpec = new JoinSpec();
    Join join = (Join) operatorFactory.getRelationRelationOperator(joinSpec);
    operators.put(joinSpec.getId(), join);
    InsertStreamSpec istrmSpec = new InsertStreamSpec();
    InsertStream istream = (InsertStream) operatorFactory.getRelationTupleOperator(istrmSpec);
    operators.put(istrmSpec.getId(), istream);
    PartitionSpec parSpec = new PartitionSpec();
    PartitionOp par = (PartitionOp) operatorFactory.getTupleTupleOperator(parSpec);
    operators.put(parSpec.getId(), par);
    SystemStreamSpec sstrmSpec = new SystemStreamSpec(this.sqlContext);
    SystemStreamOp sysStream = (SystemStreamOp) operatorFactory.getTupleOperator(sstrmSpec);
    operators.put(sstrmSpec.getId(), sysStream);

    store = new SQLRelationStore(context);

    wnd1.init(store);
    wnd2.init(store);
    join.init(store);
    istream.init(store);
    par.init(store);
    sysStream.init(store);

    wnd1.setNextOp(join);
    wnd2.setNextOp(join);
    join.setNextOp(istream);
    istream.setNextOp(par);
    par.setNextOp(sysStream);
  }
}

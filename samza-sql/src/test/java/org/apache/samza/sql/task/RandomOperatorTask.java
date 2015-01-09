package org.apache.samza.sql.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.data.SystemInputTuple;
import org.apache.samza.sql.operators.relation.Join;
import org.apache.samza.sql.operators.window.BoundedTimeWindow;
import org.apache.samza.sql.store.SqlContextManager;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;


/***
 * This example illustrate a SQL join operation that joins two streams together using the folowing operations:
 * <p>a. the two streams are each processed by a window operator to convert to relations
 * <p>b. a join operator is applied on the two relations to generate join results
 * <p>c. finally, the join results are sent out to the system output
 *
 */
public class RandomOperatorTask implements StreamTask, InitableTask, WindowableTask {
  private SqlContextManager initCntx;
  private KeyValueStore<String, List<Object>> opOutputStore;
  private BoundedTimeWindow wndOp1;
  private BoundedTimeWindow wndOp2;
  private Join joinOp;

  private BoundedTimeWindow getWindowOp(String streamName) {
    if (streamName.equals("kafka:stream1")) {
      return this.wndOp1;
    } else if (streamName.equals("kafka:stream2")) {
      return this.wndOp2;
    }

    throw new IllegalArgumentException("No window operator found for stream: " + streamName);
  }

  private void processJoinOutput(List<Object> outputs, MessageCollector collector) {
    // get each tuple in the join operator's outputs and send it to system stream
    for (Object joinOutput : outputs) {
      for (Iterator<Tuple> iter = ((Relation) joinOutput).iterator(); iter.hasNext();) {
        Tuple otuple = iter.next();
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "joinOutput1"), otuple.getKey(), otuple
            .getMessage()));
      }
    }
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub
    StoredRuntimeContext context = new StoredRuntimeContext(collector, coordinator, this.opOutputStore);

    SystemInputTuple ituple = new SystemInputTuple(envelope);

    BoundedTimeWindow wndOp = getWindowOp(ituple.getStreamName());

    wndOp.process(ituple, context);
    List<Object> wndOutputs = context.removeOutput(wndOp.getId());
    if (wndOutputs.isEmpty()) {
      return;
    }

    // process all output from the window operator
    for (Object input : wndOutputs) {
      Relation relation = (Relation) input;
      this.joinOp.process(relation, context);
    }
    // get the output from the join operator and send them
    processJoinOutput(context.removeOutput(this.joinOp.getId()), collector);

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub
    StoredRuntimeContext context = new StoredRuntimeContext(collector, coordinator, this.opOutputStore);
    long sysTimeNano = System.nanoTime();

    this.wndOp1.timeout(sysTimeNano, context);
    this.wndOp2.timeout(sysTimeNano, context);

    for (Object input : context.removeOutput(this.wndOp1.getId())) {
      Relation relation = (Relation) input;
      this.joinOp.process(relation, context);
    }

    for (Object input : context.removeOutput(this.wndOp2.getId())) {
      Relation relation = (Relation) input;
      this.joinOp.process(relation, context);
    }

    // get the output from the join operator and send them
    processJoinOutput(context.removeOutput(this.joinOp.getId()), collector);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
    // 1. create a fixed length 10 sec window operator
    this.wndOp1 = new BoundedTimeWindow("wndOp1", 10, "kafka:stream1", "relation1");
    this.wndOp2 = new BoundedTimeWindow("wndOp2", 10, "kafka:stream2", "relation2");
    // 2. create a join operation
    List<String> inputRelations = new ArrayList<String>();
    inputRelations.add("relation1");
    inputRelations.add("relation2");
    List<String> joinKeys = new ArrayList<String>();
    joinKeys.add("key1");
    joinKeys.add("key2");
    this.joinOp = new Join("joinOp", inputRelations, "joinOutput", joinKeys);
    // Finally, initialize all operators
    this.initCntx = new SqlContextManager(context);
    this.opOutputStore = (KeyValueStore<String, List<Object>>) context.getStore("samza-sql-operator-output-kvstore");
    this.wndOp1.init(initCntx);
    this.wndOp2.init(initCntx);
    this.joinOp.init(this.initCntx);
  }

}

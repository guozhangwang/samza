package org.apache.samza.sql.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.sql.api.data.Relation;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.sql.operators.relation.Join;
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


public class RandomOperatorTask implements StreamTask, InitableTask, WindowableTask {
  private SqlContextManager initCntx;
  private KeyValueStore<String, List<Object>> opOutputStore;
  private Join joinOp;

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    // TODO Auto-generated method stub
    StoredRuntimeContext context = new StoredRuntimeContext(collector, coordinator, this.opOutputStore);

    Relation inputRelation;
    if (envelope.getSystemStreamPartition().getStream().equals("relation1")) {
      inputRelation = this.initCntx.getRelation("relation1");
    } else {
      inputRelation = this.initCntx.getRelation("relation2");
    }
    joinOp.process(inputRelation, context);

    // get the output
    List<Object> joinOutputs = context.removeOutput(joinOp.getId());

    for (Object joinOutput : joinOutputs) {
      for (Iterator<Tuple> iter = ((Relation) joinOutput).iterator(); iter.hasNext();) {
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "joinOutput1"), iter.next().getMessage()));
      }
    }

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    // TODO Auto-generated method stub

  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    // TODO Auto-generated method stub
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
    this.joinOp.init(this.initCntx);
  }

}

package org.apache.samza.sql.api.data;

import java.util.Iterator;


/**
 * This class defines the interface of <code>Relation</code>, which is defined as a map of <code>Tuple</code>.
 *
 * <p>The interface defines the set of common operations on a bag of <code>Tuple</code>s that are supported,
 * including <code>get</code>, <code>put</code>, <code>delete</code>, and <code>iterator</code> that allows to iterate through all tuples.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */

public interface Relation {

  /**
   * Retrieves the tuple from the relation
   *
   * @param key
   *     The primary key for the tuple to be retrieved
   * @return
   *     The tuple corresponding to the <code>key</code>
   */
  public Tuple get(Object key);

  /**
   * Store the tuple into the relation
   *
   * @param key
   *     The primary key for the tuple to be stored
   * @param tuple
   *     The tuple to be stored in the stored relation
   */
  public void put(Object key, Tuple tuple);

  /**
   * Delete the tuple from the relation
   *
   * @param key
   *     The primary key for the tuple to be stored
   * @return
   *     The tuple to be stored in the stored relation
   */
  public Tuple delete(Object key);

  /**
   * Returns an iterator of the relation
   *
   * <p>Note that some relations stored in the remote database may not be able to support this function.
   *
   * @return
   *     The tuple to be stored in the stored relation
   */
  public Iterator<Tuple> iterator();

  /**
   * Returns the specification of this relation
   *
   * @return
   *     the specification of the relation
   */
  public RelationSpec getSpec();
}

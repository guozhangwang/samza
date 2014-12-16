package org.apache.samza.sql.api.data;

import java.util.List;

import org.apache.samza.sql.operators.window.WindowState;


/**
 * This class implements a <code>RelationStore</code> to access various types of states needed in the stream SQL operators,
 * including relations generated as intermediate results in the SQL query, the windowing operators' internal window states,
 * and the relations that are stored and backed in external or local databases.
 *
 * @author Yi Pan {yipan@linkedin.com}
 *
 */
public interface RelationStore {

  /**
   * get a relation based on its specification from the underlying storage layer/changelog
   *
   * @param spec
   *     the specification of the relation to be opened and returned
   * @return
   *     the relation object that is corresponding to the name
   */
  public Relation getRelation(RelationSpec spec);

  /**
   * get a list of window states based on the <code>wndName</code> from the underlying storage layer/changelog
   *
   * @param wndName
   *     The window operator's name. It has to be unique in the whole task
   * @return
   *     The list of current window states, including some previously closed windows and the current open window.
   */
  public List<WindowState> getWindowStates(String wndName);

}

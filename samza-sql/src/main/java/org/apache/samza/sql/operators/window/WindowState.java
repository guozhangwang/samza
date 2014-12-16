package org.apache.samza.sql.operators.window;

public class WindowState {
  public String startOffset = null;
  public String endOffset = null;
  public boolean isClosed = false;

  public void open(String offset) {
    this.isClosed = false;
    this.startOffset = offset;
  }

  public void close(String offset) {
    this.endOffset = offset;
    this.isClosed = true;
  }

  public void advanceTo(String offset) {
    this.endOffset = offset;
  }

  public boolean isClosed() {
    return this.isClosed;
  }
}

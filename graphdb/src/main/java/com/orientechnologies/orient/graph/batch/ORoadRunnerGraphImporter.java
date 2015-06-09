package com.orientechnologies.orient.graph.batch;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by luigidellaquila on 08/06/15.
 */
public class ORoadRunnerGraphImporter {

  private final String password;
  private final String username;
  private final String url;
  private int          cores = Runtime.getRuntime().availableProcessors();

  BlockingQueue<Op>    queue = new LinkedBlockingQueue<Op>();

  public interface Op {
    public boolean execute();
  }

  class EndOp implements Op {
    public boolean execute() {
      return false;
    }
  }

  class CreateEdgeOp implements Op {
    public boolean execute() {
      return false;
    }
  }

  class CreateVertexOp implements Op {
    public boolean execute() {
      return false;
    }
  }

  class QueueThread extends Thread {

    public void run() {
      while (true) {
        try {
          Op op = queue.take();
          if (op instanceof EndOp) {
            return;
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

  }

  public ORoadRunnerGraphImporter(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;
    start();
  }

  private void start() {

    for (int i = 0; i < cores; i++) {
      new QueueThread().start();
    }
  }

  public void createVertexType(String name) {
    createVertexType(name, "V");
  }

  public void createVertexType(String name, String superclass) {
    createType(name, superclass);
  }

  public void createEdgeType(String name) {
    createEdgeType(name, "E");
  }

  public void createEdgeType(String name, String superclass) {
    createType(name, superclass);
  }

  public ORoadRunnerGraphImporter(String url) {
    this(url, "admin", "admin");
  }

  public void addVertex(Object iVertexId, String type, Map<String, Object> properties) {

  }

  public void addEdge(Object iFromId, String fromType, Object iToId, String toType, String edgeType, Map<String, Object> properties) {

  }

  public void addEdge(Object iFromId, String fromType, Object iToId, String toType, String edgeType) {

  }

  private void createType(String name, String superclass) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(username, password);
    try {
      OSchemaProxy schema = db.getMetadata().getSchema();
      OClass superclazz = schema.getClass(superclass);
      schema.createClass(name, superclazz);
    } finally {
      db.close();
    }
  }

}

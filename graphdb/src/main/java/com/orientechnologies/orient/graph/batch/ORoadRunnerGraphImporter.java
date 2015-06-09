package com.orientechnologies.orient.graph.batch;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.OIndexManagerProxy;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by luigidellaquila on 08/06/15.
 */
public class ORoadRunnerGraphImporter {

  private final String password;
  private final String username;
  private final String url;
  private int          cores             = Runtime.getRuntime().availableProcessors();
  private int          nThreads          = cores;

  private String       vertexIdFieldName = "_id";
  private OType        vertexIdType      = OType.STRING;

  BlockingQueue<Op>    queue             = new LinkedBlockingQueue<Op>();
  Set<String>          edgeClasses       = new HashSet<String>();

  public interface Op {
    void execute(ODatabaseDocument db, Map<String, OIndex<?>> indexes);
  }

  class EndOp implements Op {

    @Override
    public void execute(ODatabaseDocument db, Map<String, OIndex<?>> indexes) {
      // do nothing
    }
  }

  class CreateEdgeOp implements Op {
    final Object              fromId;
    final String              fromType;
    final Object              toId;
    final String              toType;
    final String              edgeType;
    final Map<String, Object> properties;

    CreateEdgeOp(Object iFromId, String fromType, Object iToId, String toType, String edgeType, Map<String, Object> properties) {
      this.fromId = iFromId;
      this.fromType = fromType;
      this.toId = iToId;
      this.toType = toType;
      this.edgeType = edgeType;
      this.properties = properties;
    }

    @Override
    public void execute(ODatabaseDocument db, Map<String, OIndex<?>> indexes) {

      OIndexManagerProxy idxMgr = db.getMetadata().getIndexManager();
      ODocument edgeDoc = db.newInstance(edgeType);

      String inIndexName = getIndexName(toType);
      OIndex<?> inIndex = indexes.get(inIndexName);
      if (inIndex == null) {
        inIndex = idxMgr.getIndex(inIndexName);
        indexes.put(inIndexName, inIndex);
      }

      OIdentifiable toRid = (OIdentifiable) inIndex.get(toId);
      if (toRid == null) {
        queue.offer(this);
        return;
      }

      String outIndexName = getIndexName(fromType);
      OIndex<?> outIndex;
      if (outIndexName.equals(inIndexName)) {
        outIndex = inIndex;
      } else {
        outIndex = indexes.get(outIndexName);
        if (outIndex == null) {
          outIndex = idxMgr.getIndex(outIndexName);
          indexes.put(outIndexName, outIndex);
        }
      }

      OIdentifiable fromRid = (OIdentifiable) outIndex.get(fromId);
      if (fromRid == null) {
        queue.offer(this);
        return;
      }

      if (properties != null) {
        edgeDoc.fromMap(properties);
      }
      edgeDoc.field("out", fromRid);
      edgeDoc.field("in", toRid);
      edgeDoc.save();

      OIndex<?> outgoingEdgesSetIndex = idxMgr.getIndex(getEdgeIndexName(edgeType, "out"));
      OIndex<?> incomingEdgesSetIndex = idxMgr.getIndex(getEdgeIndexName(edgeType, "in"));

      outgoingEdgesSetIndex.put(fromRid, edgeDoc.getIdentity());
      incomingEdgesSetIndex.put(toRid, edgeDoc.getIdentity());
    }
  }

  class CreateVertexOp implements Op {
    final Object              id;
    final String              type;
    final Map<String, Object> properties;

    CreateVertexOp(Object id, String type, Map<String, Object> properties) {
      this.id = id;
      this.type = type;
      this.properties = properties;
    }

    @Override
    public void execute(ODatabaseDocument db, Map<String, OIndex<?>> indexes) {
      ODocument doc = db.newInstance(type);
      if (properties != null) {
        doc.fromMap(properties);
      }
      doc.field(vertexIdFieldName, id);
      doc.save();
    }
  }

  class QueueThread extends Thread {

    Map<String, OIndex<?>> indexes = new HashMap<String, OIndex<?>>();

    public void run() {
      ODatabaseDocument db = new ODatabaseDocumentTx(url);
      db.open(username, password);
      try {
        while (true) {
          try {
            Op op = queue.take();
            if (op instanceof EndOp) {
              return;
            }
            op.execute(db, indexes);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      } finally {
        db.close();
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
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);

    try {
      try {
        if (!db.exists()) {
          db.create();
        }
      } catch (Exception e) {
      }
      db.open(username, password);
    } finally {
      db.close();
    }

    createType("V", null, true);
    createType("E", null, false);
    for (int i = 0; i < nThreads; i++) {
      new QueueThread().start();
    }
  }

  public void createVertexType(String name) {
    createVertexType(name, "V");
  }

  public void createVertexType(String name, String superclass) {
    createType(name, superclass, true);
  }

  public void createEdgeType(String name) {
    createEdgeType(name, "E");
  }

  public void createEdgeType(String name, String superclass) {
    createType(name, superclass, false);
    edgeClasses.add(name);
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(username, password);
    try {
      OIndexManagerProxy idxMgr = db.getMetadata().getIndexManager();
      idxMgr.createIndex(getEdgeIndexName(name, "out"), "NOTUNIQUE_HASH_INDEX", null, null, null, null);
      idxMgr.createIndex(getEdgeIndexName(name, "in"), "NOTUNIQUE_HASH_INDEX", null, null, null, null);
    } finally {
      db.close();
    }
  }

  private String getEdgeIndexName(String name, String direction) {
    return "__ROADRUNNER_" + name + "__" + direction;
  }

  public ORoadRunnerGraphImporter(String url) {
    this(url, "admin", "admin");
  }

  public void addVertex(Object iVertexId, String type) {
    queue.offer(new CreateVertexOp(iVertexId, type, null));
  }

  public void addVertex(Object iVertexId, String type, Map<String, Object> properties) {
    queue.offer(new CreateVertexOp(iVertexId, type, properties));
  }

  public void addEdge(Object iFromId, String fromType, Object iToId, String toType, String edgeType, Map<String, Object> properties) {
    queue.offer(new CreateEdgeOp(iFromId, fromType, iToId, toType, edgeType, properties));
  }

  public void addEdge(Object iFromId, String fromType, Object iToId, String toType, String edgeType) {
    addEdge(iFromId, fromType, iToId, toType, edgeType, null);
  }

  private void createType(String name, String superclass, boolean index) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(username, password);
    try {
      OSchemaProxy schema = db.getMetadata().getSchema();

      OClass clazz;
      if (schema.existsClass(name)) {
        clazz = schema.getClass(name);
      } else {
        if (superclass != null) {
          if (!schema.existsClass(superclass)) {
            schema.createClass(superclass);
          }
          OClass superclazz = schema.getClass(superclass);
          clazz = schema.createClass(name, superclazz);
        } else {
          clazz = schema.createClass(name);
        }
      }
      if (index) {
        clazz.createProperty(vertexIdFieldName, vertexIdType);
        clazz.createIndex(getIndexName(name), OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, vertexIdFieldName);
      }
    } finally {
      db.close();
    }
  }

  private String getIndexName(String name) {
    return name + "." + vertexIdFieldName;
  }

  public void close() {
    for (int i = 0; i < nThreads - 1; i++) {
      queue.offer(new EndOp());
    }
    while (!queue.isEmpty()) {
      try {
        Thread.sleep(100);// TODO replace this
      } catch (InterruptedException e) {
        // e.printStackTrace();
      }
    }
    rebuildVertices();
  }

  private void rebuildVertices() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    db.open(username, password);

    try {
      for (String edgeType : this.edgeClasses) {
        populateEdges(edgeType, "out", db);
        populateEdges(edgeType, "in", db);
      }
    } finally {
      db.close();
    }
  }

  private void populateEdges(String edgeType, String direction, ODatabaseDocumentTx db) {
    OIndexManagerProxy idxMgr = db.getMetadata().getIndexManager();
    OIndex<?> outgoingEdgesSetIndex = idxMgr.getIndex(getEdgeIndexName(edgeType, direction));
    OIndexKeyCursor cursor = outgoingEdgesSetIndex.keyCursor();
    OIdentifiable nextKey = (OIdentifiable) cursor.next(100);
    while (nextKey != null) {
      ODocument vertexDoc = db.load(nextKey.getIdentity());
      ORidBag bag = new ORidBag();
      Iterable edgeRids = (Iterable) outgoingEdgesSetIndex.get(nextKey);
      for (Object o : edgeRids) {
        bag.add((OIdentifiable) o);
      }
      vertexDoc.field(direction + "_" + edgeType, bag);
      vertexDoc.save();
      nextKey = (OIdentifiable) cursor.next(100);
    }
  }

}

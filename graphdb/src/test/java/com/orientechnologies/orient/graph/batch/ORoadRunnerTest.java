package com.orientechnologies.orient.graph.batch;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

/**
 * Created by luigidellaquila on 09/06/15.
 */
public class ORoadRunnerTest {

  @Test
  public void test1() {
    OrientGraph g = new OrientGraph("memory:RoadRunner_test1");
    g.shutdown();

    ORoadRunnerGraphImporter importer = new ORoadRunnerGraphImporter("memory:RoadRunner_test1", "admin", "admin");
    importer.createVertexType("Person");
    importer.createEdgeType("Friend");

    importer.addVertex("andrey", "Person");
    importer.addVertex("colin", "Person");
    importer.addVertex("emanuele", "Person");
    importer.addVertex("enrico", "Person");
    importer.addVertex("luca", "Person");
    importer.addVertex("luigi", "Person");

    importer.addEdge("luigi", "Person", "andrey", "Person", "Friend");
    importer.addEdge("luigi", "Person", "colin", "Person", "Friend");
    importer.addEdge("luigi", "Person", "emanuele", "Person", "Friend");
    importer.addEdge("luigi", "Person", "enrico", "Person", "Friend");
    importer.addEdge("luigi", "Person", "luca", "Person", "Friend");

    importer.close();

    g = new OrientGraph("memory:RoadRunner_test1");
    try {
      Iterable<Vertex> result = g.command(new OSQLSynchQuery<Vertex>("select expand(out('Friend')) from person where _id = ?"))
          .execute("luigi");
      int friends = 0;
      boolean foundEnrico = false;
      for (Vertex friend : result) {
        if ("enrico".equals(friend.getProperty("_id"))) {
          foundEnrico = true;
        }
        friends++;
      }
      Assert.assertTrue(foundEnrico);
      Assert.assertEquals(friends, 5);

      result = g.command(new OSQLSynchQuery<Vertex>("select expand(in('Friend')) from person where _id = ?")).execute("luca");

      friends = 0;

      for (Vertex friend : result) {
        friends++;
      }

      Assert.assertEquals(friends, 1);
    } finally {
      g.shutdown();
    }

  }
}

package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static com.tinkerpop.gremlin.process.graph.traversal.__.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class HasTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_valuesXnameX();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_markoX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_blahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXblahX();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id);

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id);

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X();

    public abstract Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id);

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXknowsX();

    public abstract Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX();

    public abstract Traversal<Vertex, Vertex> get_g_V_hasXname_equalspredicate_markoX();

    public abstract Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age();

    public abstract Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id);

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXknowsX_hasXoutXcreatedXX_valuesXnameX() {
        Traversal<Vertex, String> traversal = get_g_V_outXknowsX_hasXoutXcreatedXX_valuesXnameX();
        printTraversalForm(traversal);
        checkResults(Arrays.asList("josh"), traversal);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXkeyX() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXkeyX(convertToVertexId("marko"), "name");
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
        //
        traversal = get_g_VX1X_hasXkeyX(convertToVertexId("marko"), "circumference");
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXname_markoX() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXname_markoX(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
        traversal = get_g_VX1X_hasXname_markoX(convertToVertexId("vadas"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_markoX();
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_blahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_blahX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXage_gt_30X() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXage_gt_30X();
        printTraversalForm(traversal);
        final List<Element> list = StreamFactory.stream(traversal).collect(Collectors.toList());
        assertEquals(2, list.size());
        for (final Element v : list) {
            assertTrue(v.<Integer>value("age") > 30);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_hasXage_gt_30X() {
        Traversal<Vertex, Vertex> traversal = get_g_VX1X_hasXage_gt_30X(convertToVertexId("marko"));
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
        traversal = get_g_VX1X_hasXage_gt_30X(convertToVertexId("josh"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_out_hasXid_2X() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_out_hasIdX2X(convertToVertexId("marko"), convertToVertexId("vadas"));
        printTraversalForm(traversal);
        assertTrue(traversal.hasNext());
        assertEquals(convertToVertexId("vadas"), traversal.next().id());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXblahX() {
        assumeTrue(graphMeetsTestRequirements());
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXblahX();
        printTraversalForm(traversal);
        assertFalse(traversal.hasNext());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void g_EX7X_hasXlabelXknowsX() {
        //System.out.println(convertToEdgeId("marko", "knows", "vadas"));
        Traversal<Edge, Edge> traversal = get_g_EX7X_hasLabelXknowsX(convertToEdgeId("marko", "knows", "vadas"));
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().label());
        }
        assertEquals(1, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_E_hasXlabelXknowsX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXknowsX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            assertEquals("knows", traversal.next().label());
        }
        assertEquals(2, counter);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_E_hasXlabelXuses_traversesX() {
        final Traversal<Edge, Edge> traversal = get_g_E_hasLabelXuses_traversesX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().label();
            assertTrue(label.equals("uses") || label.equals("traverses"));
        }
        assertEquals(9, counter);
    }

    @Test
    @LoadGraphWith(CREW)
    public void g_V_hasXlabelXperson_software_blahX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasLabelXperson_software_blahX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final String label = traversal.next().label();
            assertTrue(label.equals("software") || label.equals("person"));
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXname_equalspredicate_markoX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_hasXname_equalspredicate_markoX();
        printTraversalForm(traversal);
        assertEquals("marko", traversal.next().<String>value("name"));
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasXperson_name_markoX_age() {
        final Traversal<Vertex, Integer> traversal = get_g_V_hasXperson_name_markoX_age();
        printTraversalForm(traversal);
        assertEquals(29, traversal.next().intValue());
        assertFalse(traversal.hasNext());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_VX1X_outE_hasXweight_inside_0_06X_inV() {
        final Traversal<Vertex, Vertex> traversal = get_g_VX1X_outE_hasXweight_inside_0_06X_inV(convertToVertexId("marko"));
        printTraversalForm(traversal);
        while (traversal.hasNext()) {
            Vertex vertex = traversal.next();
            assertTrue(vertex.value("name").equals("vadas") || vertex.value("name").equals("lop"));
        }
        assertFalse(traversal.hasNext());
    }

    public static class StandardTest extends HasTest {
        public StandardTest() {
            requiresGraphComputer = false;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_valuesXnameX() {
            return g.V().out("knows").has(out("created")).values("name");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            return g.V(v1Id).has(key);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            return g.V(v1Id).has("name", "marko");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().has("blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).has("age", Compare.gt, 30);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().hasId(v2Id);
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            return g.V().has("age", Compare.gt, 30);
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            return g.E(e7Id).hasLabel("knows");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            return g.E().hasLabel("knows");
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            return g.E().hasLabel("uses", "traverses");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            return g.V().hasLabel("person", "software", "blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_equalspredicate_markoX() {
            return g.V().has("name", (a, b) -> a.equals(b), "marko");
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            return g.V().has("person", "name", "marko").values("age");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            return g.V(v1Id).outE().has("weight", Compare.inside, Arrays.asList(0.0d, 0.6d)).inV();
        }
    }

    public static class ComputerTest extends HasTest {
        public ComputerTest() {
            requiresGraphComputer = true;
        }

        @Override
        public Traversal<Vertex, String> get_g_V_outXknowsX_hasXoutXcreatedXX_valuesXnameX() {
            return g.V().out("knows").has(out("created")).<String>values("name").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXkeyX(final Object v1Id, final String key) {
            return g.V(v1Id).has(key).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXname_markoX(final Object v1Id) {
            return g.V(v1Id).has("name", "marko").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_markoX() {
            return g.V().has("name", "marko").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_blahX() {
            return g.V().has("name", "blah").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXblahX() {
            return g.V().has("blah").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_hasXage_gt_30X(final Object v1Id) {
            return g.V(v1Id).has("age", Compare.gt, 30).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_out_hasIdX2X(final Object v1Id, final Object v2Id) {
            return g.V(v1Id).out().hasId(v2Id).submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXage_gt_30X() {
            return g.V().has("age", Compare.gt, 30).submit(g.compute());
        }

        @Override
        public Traversal<Edge, Edge> get_g_EX7X_hasLabelXknowsX(final Object e7Id) {
            return g.E(e7Id).hasLabel("knows").submit(g.compute());
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXknowsX() {
            return g.E().hasLabel("knows").submit(g.compute());
        }

        @Override
        public Traversal<Edge, Edge> get_g_E_hasLabelXuses_traversesX() {
            return g.E().hasLabel("uses", "traverses").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasLabelXperson_software_blahX() {
            return g.V().hasLabel("person", "software", "blah");
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_V_hasXname_equalspredicate_markoX() {
            return g.V().has("name", (a, b) -> a.equals(b), "marko").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_hasXperson_name_markoX_age() {
            return g.V().has("person", "name", "marko").<Integer>values("age").submit(g.compute());
        }

        @Override
        public Traversal<Vertex, Vertex> get_g_VX1X_outE_hasXweight_inside_0_06X_inV(final Object v1Id) {
            return g.V(v1Id).outE().has("weight", Compare.inside, Arrays.asList(0.0d, 0.6d)).inV().submit(g.compute());
        }
    }
}
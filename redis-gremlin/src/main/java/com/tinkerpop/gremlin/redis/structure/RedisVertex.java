package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisVertex extends RedisElement implements Vertex, Vertex.Iterators {
    private static final Object[] EMPTY_ARGS = new Object[0];

    // Load vertex from database
    protected RedisVertex(final Object id, final RedisGraph graph) {
        super(id, graph);
    }

    // Create new vertex
    private RedisVertex(final String label, final RedisGraph graph) {
        super(label, graph);

        graph.getDatabase().zadd("graph::" + String.valueOf(graph.getId()) + "::vertices", (Long) id, String.valueOf(id));
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        Edge edge = new RedisEdge(vertex, label, this, (RedisGraph)graph());

        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.attachProperties(edge, keyValues);

        return edge;
    }

    @Override
    public <V> VertexProperty<V> property(String key, final V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.validateProperty(key, value);

        RedisVertexProperty prop = new RedisVertexProperty(this, key, value);

        graph.getDatabase().hset("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::property_key_to_id",
                key, String.valueOf(prop.id()));

        return prop;
    }

    @Override
    public <V> VertexProperty<V> property(String key) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, id);

        Long prop_id = Long.valueOf(graph.getDatabase().hget("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::property_key_to_id",
                key));

        return new RedisVertexProperty(prop_id, this, key);
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, id);

        super.remove();

        // TODO: Remove properties
        // TODO: Element also needs to remove properties

        graph.getDatabase().del("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::edges_in");
        graph.getDatabase().del("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::edges_out");
        graph.getDatabase().zrem("graph::" + String.valueOf(graph.getId()) + "::vertices", String.valueOf(id));
        graph.getDatabase().del("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::property_key_to_id");
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    //////////////////////////////////////////////

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        Set<String> keys = new HashSet<String>();

        if (propertyKeys.length == 0) {
            keys = graph.getDatabase().hkeys("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::properties");
        }
        else {
            for (int i = 0; i < propertyKeys.length; i++)
                keys.add(propertyKeys[i]);
        }

        return new RedisVertexPropertyIterator<V>(graph, this, keys);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        Set<String> edges = new HashSet<String>();

        if (edgeLabels.length == 0) {
            // TODO: Probably wasteful, could just assign to edges
            if (direction == Direction.IN || direction == Direction.BOTH)
                edges.addAll(graph.getDatabase().zrange("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::edges_in", 0, -1));

            if (direction == Direction.OUT || direction == Direction.BOTH)
                edges.addAll(graph.getDatabase().zrange("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::edges_out", 0, -1));
        }
        else {
            // Reverse lookup from edge labels to edge IDs
            for (int i = 0; i < edgeLabels.length; i++) {
                String label = edgeLabels[i];
                String id = graph.getDatabase().hget("graph::" + String.valueOf(graph.getId()) + "::edge_label_to_id", label);
                edges.add(id);
            }
        }

        return new RedisEdgeIterator(graph, edges);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        List<Vertex> adjacent = new ArrayList<Vertex>();

        Iterator<Edge> edgeIt = edgeIterator(direction, edgeLabels);

        while (edgeIt.hasNext()) {
            Edge edge = edgeIt.next();

            // TODO: Figure out which direction to ask for to get only the adjacent vertex
            Iterator<Vertex> vertIt = edge.iterators().vertexIterator(Direction.BOTH);

            while (vertIt.hasNext()) {
                Vertex v = vertIt.next();

                if (v.id() != id())
                    adjacent.add(v);
            }
        }

        return adjacent.iterator();
    }
}

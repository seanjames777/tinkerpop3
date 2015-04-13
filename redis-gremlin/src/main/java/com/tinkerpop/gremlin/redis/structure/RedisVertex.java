package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisVertex extends RedisElement implements Vertex, Vertex.Iterators {
    private static final Object[] EMPTY_ARGS = new Object[0];

    // Load vertex from database
    protected RedisVertex(final Object id, final RedisGraph graph) {
        super(id,
                graph.getDatabase().get("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label"),
                graph);
    }

    // Create new vertex
    protected RedisVertex(final String label, final RedisGraph graph) {
        super(graph.getDatabase().incr("graph::" + String.valueOf(graph.getId()) + "::next_vertex_id"),
                label, graph);

        graph.getDatabase().set("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label", label);
        graph.getDatabase().zadd("graph::" + String.valueOf(graph.getId()) + "::vertices", (Long)id, String.valueOf(id));

    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        return new RedisEdge(vertex, label, this, (RedisGraph)graph());

        // TODO: Properties
    }

    @Override
    public void remove() {
        graph.getDatabase().del("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label");
        graph.getDatabase().del("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::edges_in");
        graph.getDatabase().del("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::edges_out");
        graph.getDatabase().zrem("graph::" + String.valueOf(graph.getId()) + "::vertices", String.valueOf(id));
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
        return (Iterator) super.propertyIterator(propertyKeys);
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
            // TODO: Need to look up by label
            return null;
        }

        return new RedisEdgeIterator(graph, edges);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        // TODO
        return null;
    }
}

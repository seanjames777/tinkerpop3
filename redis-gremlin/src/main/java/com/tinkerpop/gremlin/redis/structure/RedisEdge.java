package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisEdge extends RedisElement implements Edge, Edge.Iterators {

    // Load edge from database
    protected RedisEdge(final Object id, final RedisGraph graph) {
        super(id,
                graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label"),
                graph);
    }

    // Create new edge
    protected RedisEdge(final Vertex outVertex, final String label, final Vertex inVertex, final RedisGraph graph) {
        super(graph.getDatabase().incr("graph::" + String.valueOf(graph.getId()) + "::next_edge_id"),
                label, graph);

        graph.getDatabase().set("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in", String.valueOf(inVertex.id()));
        graph.getDatabase().set("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out", String.valueOf(outVertex.id()));
        graph.getDatabase().set("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label", label);

        graph.getDatabase().hset("graph::" + String.valueOf(graph.getId()) + "::edge_label_to_id", label, String.valueOf(id));

        graph.getDatabase().zadd("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(inVertex.id()) + "::edges_in", (Long)id, String.valueOf(id));
        graph.getDatabase().zadd("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(outVertex.id()) + "::edges_out", (Long)id, String.valueOf(id));

        graph.getDatabase().zadd("graph::" + String.valueOf(graph.getId()) + "::edges", (Long)id, String.valueOf(id));
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final Property oldProperty = super.property(key);
        final Property<V> newProperty = new RedisProperty<>(this, key, value);
        this.properties.put(key, Collections.singletonList(newProperty));
        return newProperty;
    }

    @Override
    public void remove() {
        graph.getDatabase().del("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in");
        graph.getDatabase().del("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out");
        graph.getDatabase().del("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label");

        String inVertex = graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in");
        String outVertex = graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out");

        graph.getDatabase().zrem("vertex::" + String.valueOf(graph.getId()) + "::" + inVertex + "::edges_in", String.valueOf(id));
        graph.getDatabase().zrem("vertex::" + String.valueOf(graph.getId()) + "::" + outVertex + "::edges_out", String.valueOf(id));

        graph.getDatabase().zrem("graph::" + String.valueOf(graph.getId()) + "::edges", String.valueOf(id));

        graph.getDatabase().hdel("graph::"+ String.valueOf(graph.getId()) + "::edge_label_to_id", label);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    //////////////////////////////////////////////

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        RedisVertex outVertex, inVertex;

        switch (direction) {
            case OUT:
                outVertex = new RedisVertex(graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out"), graph);
                return IteratorUtils.of(outVertex);
            case IN:
                inVertex = new RedisVertex(graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in"), graph);
                return IteratorUtils.of(inVertex);
            default:
                outVertex = new RedisVertex(graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out"), graph);
                inVertex = new RedisVertex(graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in"), graph);
                return IteratorUtils.of(outVertex, inVertex);
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}

package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.*;
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
        super(id, graph);
    }

    // Create new edge
    protected RedisEdge(final Vertex outVertex, final String label, final Vertex inVertex, final RedisGraph graph) {
        super(label, graph);

        graph.getDatabase().set("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in", String.valueOf(inVertex.id()));
        graph.getDatabase().set("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out", String.valueOf(outVertex.id()));

        graph.getDatabase().hset("graph::" + String.valueOf(graph.getId()) + "::edge_label_to_id", label, String.valueOf(id));

        graph.getDatabase().zadd("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(inVertex.id()) + "::edges_in", (Long)id, String.valueOf(id));
        graph.getDatabase().zadd("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(outVertex.id()) + "::edges_out", (Long) id, String.valueOf(id));

        graph.getDatabase().zadd("graph::" + String.valueOf(graph.getId()) + "::edges", (Long) id, String.valueOf(id));
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, id);
        this.removed = true;

        String label = label();

        super.remove();

        graph.getDatabase().del("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in");
        graph.getDatabase().del("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out");

        String inVertex = graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_in");
        String outVertex = graph.getDatabase().get("edge::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::vertex_out");

        graph.getDatabase().zrem("vertex::" + String.valueOf(graph.getId()) + "::" + inVertex + "::edges_in", String.valueOf(id));
        graph.getDatabase().zrem("vertex::" + String.valueOf(graph.getId()) + "::" + outVertex + "::edges_out", String.valueOf(id));

        graph.getDatabase().zrem("graph::" + String.valueOf(graph.getId()) + "::edges", String.valueOf(id));

        graph.getDatabase().del("graph::" + String.valueOf(graph.getId()) + "::edge_label_to_id", label);
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

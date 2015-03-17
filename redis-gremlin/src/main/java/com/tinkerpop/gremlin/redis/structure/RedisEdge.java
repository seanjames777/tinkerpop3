package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisEdge extends RedisElement implements Edge, Edge.Iterators {

    protected final Vertex inVertex;
    protected final Vertex outVertex;

    protected RedisEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final RedisGraph graph) {
        super(id, label, graph);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
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
        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id);
        final RedisVertex outVertex = (RedisVertex) this.outVertex;
        final RedisVertex inVertex = (RedisVertex) this.inVertex;

        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }

        this.graph.edges.remove(this.id());
        this.properties.clear();
        this.removed = true;
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
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}

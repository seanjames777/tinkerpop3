package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisVertexProperty<V> extends RedisElement implements VertexProperty<V>, VertexProperty.Iterators {

    private final RedisVertex vertex;
    private final String key;

    public RedisVertexProperty(final RedisVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(key, vertex.graph);

        this.vertex = vertex;
        this.key = key;

        graph.getDatabase().hset("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(vertex.id()) + "::properties",
                key, (String)value);

        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public RedisVertexProperty(final Object id, final RedisVertex vertex, final String key) {
        super(id, vertex.graph);

        this.vertex = vertex;
        this.key = key;
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(VertexProperty.class, id);
        this.removed = true;

        super.remove();

        graph.getDatabase().hdel("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(vertex.id()) + "::properties",
                key);
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public V value() {
        return (V)graph.getDatabase().hget("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(vertex.id()) + "::properties",
                key);
    }

    @Override
    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public boolean isPresent() {
        return value() != null;
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}

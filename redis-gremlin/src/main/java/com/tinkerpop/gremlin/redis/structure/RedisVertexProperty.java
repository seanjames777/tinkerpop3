package com.tinkerpop.gremlin.redis.structure;

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
    private final V value;

    public RedisVertexProperty(final RedisVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(RedisHelper.getNextId(vertex.graph), key, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public RedisVertexProperty(final Object id, final RedisVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        final Property<U> property = new RedisProperty<U>(this, key, value);
        this.properties.put(key, Collections.singletonList(property));
        return property;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        if (this.vertex.properties.containsKey(this.key)) {
            this.vertex.properties.get(this.key).remove(this);
            if (this.vertex.properties.get(this.key).size() == 0) {
                this.vertex.properties.remove(this.key);
            }
            final AtomicBoolean delete = new AtomicBoolean(true);
            this.vertex.propertyIterator(this.key).forEachRemaining(property -> {
                if (property.value().equals(this.value))
                    delete.set(false);
            });
            this.properties.clear();
            this.removed = true;
        }
    }

    //////////////////////////////////////////////

    public VertexProperty.Iterators iterators() {
        return this;
    }

    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}

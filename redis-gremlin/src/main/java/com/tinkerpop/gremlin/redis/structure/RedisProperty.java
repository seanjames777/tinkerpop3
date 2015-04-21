package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisProperty<V> implements Property<V> {

    protected final Element element;
    protected final String key;
    protected final RedisGraph graph;
    protected boolean removed = false;

    // Create new property
    public RedisProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.graph = ((RedisElement) this.element).graph;

        graph.getDatabase().hset("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(element.id()) + "::properties",
            key, (String)value);
    }

    // Lookup property
    public RedisProperty(final Element element, final String key) {
        this.element = element;
        this.key = key;
        this.graph = ((RedisElement) this.element).graph;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return (V)graph.getDatabase().hget("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(element.id()) + "::properties",
                key);
    }

    @Override
    public boolean isPresent() {
        return value() != null;
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        if (removed)
            return;

        removed = true;

        graph.getDatabase().hdel("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(element.id()) + "::properties",
                key);
    }
}

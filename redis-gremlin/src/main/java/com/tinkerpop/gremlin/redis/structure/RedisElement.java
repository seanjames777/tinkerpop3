package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class RedisElement implements Element, Element.Iterators {

    protected final Long id;
    protected final RedisGraph graph;
    protected boolean removed = false;
    protected boolean exists = true;

    protected RedisElement(final Long id, final RedisGraph graph) {
        this.graph = graph;
        this.id = id;

        String shouldHaveId = graph.getDatabase().get("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::id");

        if (shouldHaveId == null)
            exists = false;
    }

    protected RedisElement(final String label, final RedisGraph graph) {
        this.id = graph.getDatabase().incr("graph::" + String.valueOf(graph.getId()) + "::next_element_id");
        this.graph = graph;

        graph.getDatabase().set("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label", label);
        graph.getDatabase().set("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::id", String.valueOf(id));
    }

    @Override
    public void remove() {
        graph.getDatabase().del("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label");
        graph.getDatabase().del("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::properties");
        graph.getDatabase().del("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::id");
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return graph.getDatabase().get("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::label");
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Set<String> keys() {
        return graph.getDatabase().hkeys("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::properties");
    }

    @Override
    public <V> Property<V> property(String key, final V value) {
        ElementHelper.validateProperty(key, value);

        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());

        if (!(value instanceof String))
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);

        RedisProperty prop = new RedisProperty(this, key, value);

        if (!prop.isPresent())
            return Property.<V>empty();

        return prop;
    }

    @Override
    public <V> Property<V> property(final String key) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());

        RedisProperty<V> property = new RedisProperty<V>(this, key);

        if (!property.isPresent())
            return Property.<V>empty();

        return property;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    //////////////////////////////////////////////

    @Override
    public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());

        Set<String> keys = new HashSet<String>();

        if (propertyKeys.length == 0) {
            keys = graph.getDatabase().hkeys("element::" + String.valueOf(graph.getId()) + "::" + String.valueOf(id) + "::properties");
        }
        else {
            for (int i = 0; i < propertyKeys.length; i++)
                keys.add(propertyKeys[i]);
        }

        return new RedisPropertyIterator<V>(graph, this, keys);
    }
}

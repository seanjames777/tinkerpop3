package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisVertex extends RedisElement implements Vertex, Vertex.Iterators {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();
    private static final Object[] EMPTY_ARGS = new Object[0];

    protected RedisVertex(final Object id, final String label, final RedisGraph graph) {
        super(id, label, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        /*if (removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);

        if (this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else*/

        // TODO
        return VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        /*
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);

        ElementHelper.validateProperty(key, value);
        final VertexProperty<V> vertexProperty = optionalId.isPresent() ?
                new RedisVertexProperty<V>(optionalId.get(), this, key, value) :
                new RedisVertexProperty<V>(this, key, value);
        final List<Property> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(vertexProperty);
        this.properties.put(key, list);
        ElementHelper.attachProperties(vertexProperty, keyValues);
        return vertexProperty;*/

        // TODO
        return VertexProperty.<V>empty();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        /*
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        return RedisHelper.addEdge(this.graph, this, (RedisVertex) vertex, label, keyValues);
        */

        // TODO
        return null;
    }

    @Override
    public void remove() {
        /*
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        final List<Edge> edges = new ArrayList<>();
        this.iterators().edgeIterator(Direction.BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> !((RedisEdge) edge).removed).forEach(Edge::remove);
        this.properties.clear();
        this.graph.vertices.remove(this.id);
        this.removed = true;
        */

        // TODO
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
        // return (Iterator) RedisHelper.getEdges(this, direction, edgeLabels);
        // TODO
        return null;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        // TODO
        return null;
        //return (Iterator) RedisHelper.getVertices(this, direction, edgeLabels);
    }
}

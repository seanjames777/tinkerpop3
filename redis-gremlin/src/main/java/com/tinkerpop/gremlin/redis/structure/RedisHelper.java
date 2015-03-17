package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisHelper {

    protected final synchronized static long getNextId(final RedisGraph graph) {
        return Stream.generate(() -> (++graph.currentId)).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
    }

    protected static Edge addEdge(final RedisGraph graph, final RedisVertex outVertex, final RedisVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = RedisHelper.getNextId(graph);
        }

        edge = new RedisEdge(idValue, outVertex, label, inVertex, graph);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
        RedisHelper.addOutEdge(outVertex, label, edge);
        RedisHelper.addInEdge(inVertex, label, edge);
        return edge;

    }

    protected static void addOutEdge(final RedisVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final RedisVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }
    
    public static Map<String, List<Property>> getProperties(final RedisElement element) {
        return element.properties;
    }

    public static final Iterator<RedisEdge> getEdges(final RedisVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.outEdges.values().forEach(edges::addAll);
            else if (edgeLabels.length == 1)
                edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
            else
                Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.inEdges.values().forEach(edges::addAll);
            else if (edgeLabels.length == 1)
                edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
            else
                Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
        }
        return (Iterator) edges.iterator();
    }

    public static final Iterator<RedisVertex> getVertices(final RedisVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((RedisEdge) edge).inVertex)));
            else if (edgeLabels.length == 1)
                vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((RedisEdge) edge).inVertex));
            else
                Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((RedisEdge) edge).inVertex));
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((RedisEdge) edge).outVertex)));
            else if (edgeLabels.length == 1)
                vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((RedisEdge) edge).outVertex));
            else
                Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((RedisEdge) edge).outVertex));
        }
        return (Iterator) vertices.iterator();
    }
}

package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * An in-sideEffects, reference implementation of the property graph interfaces provided by Gremlin3.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)

public class RedisGraph implements Graph, Graph.Iterators {

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, RedisGraph.class.getName());
    }};

    protected Long currentId = -1l;
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();
    protected RedisGraphVariables variables = new RedisGraphVariables();

    /**
     * An empty private constructor that initializes {@link RedisGraph} with no {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy}.  Primarily
     * used for purposes of serialization issues.
     */
    private RedisGraph() {
    }

    /**
     * Open a new {@link RedisGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> If a {@link com.tinkerpop.gremlin.structure.Graph } implementation does not require a
     * {@link org.apache.commons.configuration.Configuration} (or perhaps has a default configuration) it can choose to implement a zero argument
     * open() method. This is an optional constructor method for RedisGraph. It is not enforced by the Gremlin
     * Test Suite.
     */
    public static RedisGraph open() {
        return open(null);
    }

    /**
     * Open a new {@link RedisGraph} instance.
     * <p/>
     * <b>Reference Implementation Help:</b> This method is the one use by the
     * {@link com.tinkerpop.gremlin.structure.util.GraphFactory} to instantiate
     * {@link com.tinkerpop.gremlin.structure.Graph} instances.  This method must be overridden for the Blueprint Test
     * Suite to pass. Implementers have latitude in terms of how exceptions are handled within this method.  Such
     * exceptions will be considered implementation specific by the test suite as all test generate graph instances
     * by way of {@link com.tinkerpop.gremlin.structure.util.GraphFactory}. As such, the exceptions get generalized
     * behind that facade and since {@link com.tinkerpop.gremlin.structure.util.GraphFactory} is the preferred method
     * to opening graphs it will be consistent at that level.
     *
     * @param configuration the configuration for the instance
     * @return a newly opened {@link com.tinkerpop.gremlin.structure.Graph}
     */
    public static RedisGraph open(final Configuration configuration) {
        return new RedisGraph();
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = RedisHelper.getNextId(this);
        }

        final Vertex vertex = new RedisVertex(idValue, label, this);
        this.vertices.put(vertex.id(), vertex);
        ElementHelper.attachProperties(vertex, keyValues);
        return vertex;
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Variables variables() {
        return this.variables;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.variables = new RedisGraphVariables();
        this.currentId = 0l;
    }

    @Override
    public void close() {

    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return EMPTY_CONFIGURATION;
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        if (0 == vertexIds.length) {
            return this.vertices.values().iterator();
        } else if (1 == vertexIds.length) {
            final Vertex vertex = this.vertices.get(vertexIds[0]);
            return null == vertex ? Collections.emptyIterator() : IteratorUtils.of(vertex);
        } else
            return Stream.of(vertexIds).map(this.vertices::get).filter(Objects::nonNull).iterator();
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        if (0 == edgeIds.length) {
            return this.edges.values().iterator();
        } else if (1 == edgeIds.length) {
            final Edge edge = this.edges.get(edgeIds[0]);
            return null == edge ? Collections.emptyIterator() : IteratorUtils.of(edge);
        } else
            return Stream.of(edgeIds).map(this.edges::get).filter(Objects::nonNull).iterator();
    }

    /**
     * Return RedisGraph feature set.
     * <p/>
     * <b>Reference Implementation Help:</b> Implementers only need to implement features for which there are
     * negative or instance configured features.  By default, all {@link Features} return true.
     */
    @Override
    public Features features() {
        return RedisGraphFeatures.INSTANCE;
    }

    public static class RedisGraphFeatures implements Features {

        static final RedisGraphFeatures INSTANCE = new RedisGraphFeatures();

        private RedisGraphFeatures() {}

        @Override
        public GraphFeatures graph() {
            return RedisGraphGraphFeatures.INSTANCE;
        }

        @Override
        public EdgeFeatures edge() {
            return RedisGraphEdgeFeatures.INSTANCE;
        }

        @Override
        public VertexFeatures vertex() {
            return RedisGraphVertexFeatures.INSTANCE;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public static class RedisGraphVertexFeatures implements Features.VertexFeatures {
        static final RedisGraphVertexFeatures INSTANCE = new RedisGraphVertexFeatures();

        private RedisGraphVertexFeatures() {}

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class RedisGraphEdgeFeatures implements Features.EdgeFeatures {
        static final RedisGraphEdgeFeatures INSTANCE = new RedisGraphEdgeFeatures();

        private RedisGraphEdgeFeatures(){}

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class RedisGraphGraphFeatures implements Features.GraphFeatures {
        static final RedisGraphGraphFeatures INSTANCE = new RedisGraphGraphFeatures();

        private RedisGraphGraphFeatures() {}

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsComputer() { return false; }
    }
}

package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;



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

    private Jedis jedis = null;

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, RedisGraph.class.getName());
    }};

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6379;
    private static final String HOST_PROPERTY = "host";
    private static final String PORT_PROPERTY = "port";

    private long graphId = -1;
    private RedisGraphVariables variables = new RedisGraphVariables(this);

    // TODO: These two methods should probably not be exposed publicly
    public long getId() {
        return graphId;
    }

    public Jedis getDatabase() {
        return jedis;
    }

    /**
     * An empty private constructor that initializes {@link RedisGraph} with no {@link com.tinkerpop.gremlin.structure.strategy.GraphStrategy}.  Primarily
     * used for purposes of serialization issues.
     */
    private RedisGraph(String host, int port) {
        jedis = new Jedis(host, port);

        graphId = jedis.incr("globals::next_graph_id");
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
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;

        if (configuration != null) {
            if (configuration.containsKey(HOST_PROPERTY))
                host = configuration.getString(HOST_PROPERTY);

            if (configuration.containsKey(PORT_PROPERTY))
                port = configuration.getInt(PORT_PROPERTY);
        }

        return new RedisGraph(host, port);
    }

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        // TODO: The reference implementation extracts a possible ID and label from the key/value pairs.
        // Because we are assigning monotonically increasing vertex/edge ID's, we don't match that behavior

        ElementHelper.legalPropertyKeyValueArray(keyValues);

        //Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        /*if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = RedisHelper.getNextId(this);
        }*/

        final Vertex vertex = new RedisVertex(label, this);

        // TODO: Support vertex properties
        // ElementHelper.attachProperties(vertex, keyValues);

        return vertex;
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    @Override
    public Variables variables() {
        return variables;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "");
    }

    public void clear() {
        for (Iterator<Edge> edges = edgeIterator(); edges.hasNext(); ) {
            Edge e = edges.next();
            e.remove();
        }

        for (Iterator<Vertex> vertices = vertexIterator(); vertices.hasNext(); ) {
            Vertex v = vertices.next();
            v.remove();
        }

        jedis.del("graph::" + String.valueOf(graphId) + "::next_element_id");
        jedis.del("graph::" + String.valueOf(graphId) + "::vertices");
        jedis.del("graph::" + String.valueOf(graphId) + "::edges");
        jedis.del("graph::" + String.valueOf(graphId) + "::variables");
        jedis.del("graph::" + String.valueOf(graphId) + "::edge_label_to_id");
    }

    @Override
    public void close() {
        clear();

        jedis.close();
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
        Set<String> ids = new HashSet<String>();

        if (vertexIds.length == 0) {
            Set<String> all = jedis.zrange("graph::" + String.valueOf(graphId) + "::vertices", 0, -1);
            ids.addAll(all);
        }
        else {
            for (Object id : vertexIds)
                ids.add((String)id);
        }

        return new RedisVertexIterator(this, ids);
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        Set<String> ids = new HashSet<String>();

        if (edgeIds.length == 0) {
            Set<String> all = jedis.zrange("graph::" + String.valueOf(graphId) + "::edges", 0, -1);
            ids.addAll(all);
        }
        else {
            for (Object id : edgeIds)
                ids.add((String)id);
        }

        return new RedisEdgeIterator(this, ids);
    }

    // TODO: Finish filling out the features below

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

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() { return false; }

        @Override
        public boolean supportsUuidIds() { return false; }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() { return false; }

        @Override
        public boolean supportsMetaProperties() { return true; }

        @Override
        public Features.VertexPropertyFeatures properties() { return RedisGraphVertexPropertyFeatures.INSTANCE; }
    }

    public static class RedisGraphEdgeFeatures implements Features.EdgeFeatures {
        static final RedisGraphEdgeFeatures INSTANCE = new RedisGraphEdgeFeatures();

        private RedisGraphEdgeFeatures(){}

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() { return false; }

        @Override
        public boolean supportsUuidIds() { return false; }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public Features.EdgePropertyFeatures properties() { return RedisGraphEdgePropertyFeatures.INSTANCE; }
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

        @Override
        public Features.VariableFeatures variables() { return RedisGraphVariableFeatures.INSTANCE; }
    }

    public static class RedisGraphDataTypeFeatures implements Features.DataTypeFeatures {
        private RedisGraphDataTypeFeatures() {}

        @Override
        public boolean supportsBooleanValues() { return false; }

        @Override
        public boolean supportsByteValues() { return false; }

        @Override
        public boolean supportsDoubleValues() { return false; }

        @Override
        public boolean supportsFloatValues() { return false; }

        @Override
        public boolean supportsIntegerValues() { return false; }

        @Override
        public boolean supportsLongValues() { return false; }

        @Override
        public boolean supportsMapValues() { return false; }

        @Override
        public boolean supportsMixedListValues() { return false; }

        @Override
        public boolean supportsBooleanArrayValues() { return false; }

        @Override
        public boolean supportsByteArrayValues() { return false; }

        @Override
        public boolean supportsDoubleArrayValues() { return false; }

        @Override
        public boolean supportsFloatArrayValues() { return false; }

        @Override
        public boolean supportsIntegerArrayValues() { return false; }

        @Override
        public boolean supportsStringArrayValues() { return false; }

        @Override
        public boolean supportsLongArrayValues() { return false; }

        @Override
        public boolean supportsSerializableValues() { return false; }

        @Override
        public boolean supportsStringValues() { return true; }

        @Override
        public boolean supportsUniformListValues() { return false; }
    }

    public static class RedisGraphVariableFeatures extends RedisGraphDataTypeFeatures implements Features.VariableFeatures {
        static final RedisGraphVariableFeatures INSTANCE = new RedisGraphVariableFeatures();

        private RedisGraphVariableFeatures() {}
    }

    public static class RedisGraphVertexPropertyFeatures extends RedisGraphDataTypeFeatures implements Features.VertexPropertyFeatures {
        static final RedisGraphVertexPropertyFeatures INSTANCE = new RedisGraphVertexPropertyFeatures();

        private RedisGraphVertexPropertyFeatures() {}

        @Override
        public boolean supportsUserSuppliedIds() { return false; }

        @Override
        public boolean supportsNumericIds() { return true; }

        @Override
        public boolean supportsStringIds() { return false; }

        @Override
        public boolean supportsUuidIds() { return false; }

        @Override
        public boolean supportsCustomIds() { return false; }

        @Override
        public boolean supportsAnyIds() { return false; }
    }

    public static class RedisGraphEdgePropertyFeatures extends RedisGraphDataTypeFeatures implements Features.EdgePropertyFeatures {
        static final RedisGraphEdgePropertyFeatures INSTANCE = new RedisGraphEdgePropertyFeatures();

        private RedisGraphEdgePropertyFeatures() {}
    }
}

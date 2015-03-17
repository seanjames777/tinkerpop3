package com.tinkerpop.gremlin.redis;

import com.tinkerpop.gremlin.AbstractGraphProvider;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.redis.structure.RedisEdge;
import com.tinkerpop.gremlin.redis.structure.RedisElement;
import com.tinkerpop.gremlin.redis.structure.RedisGraph;
import com.tinkerpop.gremlin.redis.structure.RedisGraphVariables;
import com.tinkerpop.gremlin.redis.structure.RedisProperty;
import com.tinkerpop.gremlin.redis.structure.RedisVertex;
import com.tinkerpop.gremlin.redis.structure.RedisVertexProperty;
import org.apache.commons.configuration.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RedisGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(RedisEdge.class);
        add(RedisElement.class);
        add(RedisGraph.class);
        add(RedisGraphVariables.class);
        add(RedisProperty.class);
        add(RedisVertex.class);
        add(RedisVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, RedisGraph.class.getName());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null)
            g.close();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }
}

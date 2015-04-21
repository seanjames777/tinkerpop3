package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RedisGraphVariables implements Graph.Variables {

    RedisGraph graph;

    public RedisGraphVariables(RedisGraph graph) {
        this.graph = graph;
    }

    @Override
    public Set<String> keys() {
        return graph.getDatabase().hkeys("graph::" + String.valueOf(graph.getId()) + "::variables");
    }

    @Override
    public <R> Optional<R> get(final String key) {
        // TODO: Jedis returns "a special 'nil' value" when the key is not found
        // TODO: Decide how to store general "object" values

        String value = graph.getDatabase().hget("graph::" + String.valueOf(graph.getId()) + "::variables", key);

        return Optional.ofNullable((R)value);
    }

    @Override
    public void remove(final String key) {
        graph.getDatabase().hdel("graph::" + String.valueOf(graph.getId()) + "::variables", key);
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);

        if (!(value instanceof String))
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value);

        graph.getDatabase().hset("graph::" + String.valueOf(graph.getId()) + "::variables", key, String.valueOf(value));
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}

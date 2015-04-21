package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by Sean on 4/11/15.
 */
public class RedisVertexPropertyIterator<V> implements Iterator<VertexProperty<V>> {

    List<VertexProperty<V>> properties = new ArrayList<VertexProperty<V>>();
    int currIdx = 0;

    public RedisVertexPropertyIterator(RedisGraph graph, RedisVertex vertex, Set<String> keys) {
        // TODO: Array list might be slow
        for (String key : keys) {
            Long prop_id = Long.valueOf(graph.getDatabase().hget("vertex::" + String.valueOf(graph.getId()) + "::" + String.valueOf(vertex.id()) + "::property_key_to_id",
                    key));
            VertexProperty<V> prop = new RedisVertexProperty(prop_id, vertex, key);
            properties.add(prop);
        }
    }

    public boolean hasNext() {
        return currIdx < properties.size();
    }

    public VertexProperty<V> next() {
        return properties.get(currIdx++);
    }

}

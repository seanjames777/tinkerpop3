package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by Sean on 4/11/15.
 */
public class RedisPropertyIterator<V> implements Iterator<Property<V>> {

    List<Property<V>> properties = new ArrayList<Property<V>>();
    int currIdx = 0;

    public RedisPropertyIterator(RedisGraph graph, RedisElement element, Set<String> keys) {
        // TODO: Array list might be slow
        for (String key : keys) {
            Property<V> prop = new RedisProperty(element, key);
            properties.add(prop);
        }
    }

    public boolean hasNext() {
        return currIdx < properties.size();
    }

    public Property<V> next() {
        return properties.get(currIdx++);
    }

}

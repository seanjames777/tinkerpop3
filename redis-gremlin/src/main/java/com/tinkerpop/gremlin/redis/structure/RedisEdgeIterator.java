package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by Sean on 4/11/15.
 */
public class RedisEdgeIterator implements Iterator<Edge> {

    List<Edge> edges = new ArrayList<Edge>();
    int currIdx = 0;

    public RedisEdgeIterator(RedisGraph graph, Set<Long> edgeIds) {
        // TODO: Array list might be slow
        for (Long id : edgeIds) {
            RedisEdge edge = new RedisEdge(id, graph);

            if (!edge.exists)
                throw Graph.Exceptions.elementNotFound(Edge.class, id);

            edges.add(edge);
        }
    }

    public boolean hasNext() {
        return currIdx < edges.size();
    }

    public Edge next() {
        return edges.get(currIdx++);
    }

}

package com.tinkerpop.gremlin.redis.structure;

        import com.tinkerpop.gremlin.structure.Vertex;

        import java.util.ArrayList;
        import java.util.Iterator;
        import java.util.List;
        import java.util.Set;

/**
 * Created by Sean on 4/11/15.
 */
public class RedisVertexIterator implements Iterator<Vertex> {

    List<Vertex> vertices = new ArrayList<Vertex>();
    int currIdx = 0;

    public RedisVertexIterator(RedisGraph graph, Set<Long> edgeIds) {
        // TODO: Array list might be slow
        for (Long id : edgeIds) {
            Vertex vertex = new RedisVertex(id, graph);
            vertices.add(vertex);
        }
    }

    public boolean hasNext() {
        return currIdx < vertices.size();
    }

    public Vertex next() {
        return vertices.get(currIdx++);
    }

}

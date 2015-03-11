package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
// TODO: add TinkerASPMemory
public class TinkerMemory implements Memory.Admin {

    public final Set<String> memoryKeys = new HashSet<>();
    public Map<String, Object> previousMap;
    public Map<String, Object> currentMap;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);

    public TinkerMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers) {
        this.currentMap = new ConcurrentHashMap<>();
        this.previousMap = new ConcurrentHashMap<>();
        if (null != vertexProgram) {
            for (final String key : vertexProgram.getMemoryComputeKeys()) {
                MemoryHelper.validateKey(key);
                this.memoryKeys.add(key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryKeys.add(mapReduce.getMemoryKey());
        }
    }

    @Override
    public Set<String> keys() {
        return this.previousMap.keySet();
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    protected void complete() {
        this.iteration.decrementAndGet();
        this.previousMap = this.currentMap;
    }

    protected void completeSubRound() {
        this.previousMap = new ConcurrentHashMap<>(this.currentMap);

    }

    @Override
    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.previousMap.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public long incr(final String key, final long delta) {
        checkKeyValue(key, delta);
        this.currentMap.compute(key, (k, v) -> null == v ? delta : delta + (Long) v);
        return (Long) this.previousMap.getOrDefault(key, 0l) + delta;
    }

    @Override
    public boolean and(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        this.currentMap.compute(key, (k, v) -> null == v ? bool : bool && (Boolean) v);
        return (Boolean) this.previousMap.getOrDefault(key, true) && bool;
    }

    @Override
    public boolean or(final String key, final boolean bool) {
        checkKeyValue(key, bool);
        this.currentMap.compute(key, (k, v) -> null == v ? bool : bool || (Boolean) v);
        return (Boolean) this.previousMap.getOrDefault(key, true) || bool;
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        this.currentMap.put(key, value);
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.contains(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }
}

package com.tinkerpop.gremlin.tinkergraph.process.computer;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.VertexProgram;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerWorkerPool {

    public static enum State {VERTEX_PROGRAM, MAP_REDUCE}

    private List<MapReduce> mapReducers;
    private List<VertexProgram> vertexPrograms;
    private State state;

    public TinkerWorkerPool(final int numberOfWorkers, final VertexProgram vertexProgram) {
        try {
            this.state = State.VERTEX_PROGRAM;
            this.vertexPrograms = new ArrayList<>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                this.vertexPrograms.add(vertexProgram.clone());
            }
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public TinkerWorkerPool(final int numberOfWorkers, final MapReduce mapReduce) {
        try {
            this.state = State.MAP_REDUCE;
            this.mapReducers = new ArrayList<>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                this.mapReducers.add(mapReduce.clone());
            }
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void executeVertexProgram(final Consumer<VertexProgram> worker) {
        if (!this.state.equals(State.VERTEX_PROGRAM))
            throw new IllegalStateException("The provided TinkerWorkerPool is not setup for VertexProgram: " + this.state);
        final CountDownLatch activeWorkers = new CountDownLatch(this.vertexPrograms.size());
        for (final VertexProgram vertexProgram : this.vertexPrograms) {
            new Thread(() -> {
                worker.accept(vertexProgram);
                activeWorkers.countDown();
            }).start();
        }
        try {
            activeWorkers.await();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void executeMapReduce(final Consumer<MapReduce> worker) {
        if (!this.state.equals(State.MAP_REDUCE))
            throw new IllegalStateException("The provided TinkerWorkerPool is not setup for MapReduce: " + this.state);
        final CountDownLatch activeWorkers = new CountDownLatch(this.mapReducers.size());
        for (final MapReduce mapReduce : this.mapReducers) {
            new Thread(() -> {
                worker.accept(mapReduce);
                activeWorkers.countDown();
            }).start();
        }
        try {
            activeWorkers.await();
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
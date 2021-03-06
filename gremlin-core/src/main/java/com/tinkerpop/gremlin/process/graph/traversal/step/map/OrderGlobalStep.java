package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.traversal.step.ComparatorHolder;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Order;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderGlobalStep<S> extends CollectingBarrierStep<S> implements Reversible, ComparatorHolder<S> {

    private final List<Comparator<S>> comparators = new ArrayList<>();

    public OrderGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setConsumer(traversers -> traversers.sort((Comparator) Order.incr));
    }

    @Override
    public void addComparator(final Comparator<S> comparator) {
        this.comparators.add(comparator);
        final Comparator<Traverser<S>> chainedComparator = this.comparators.stream().map(c -> (Comparator<Traverser<S>>) new Comparator<Traverser<S>>() {
            @Override
            public int compare(final Traverser<S> traverserA, final Traverser<S> traverserB) {
                return c.compare(traverserA.get(), traverserB.get());
            }
        }).reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(chainedComparator));
    }

    @Override
    public List<Comparator<S>> getComparators() {
        return this.comparators;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparators);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}

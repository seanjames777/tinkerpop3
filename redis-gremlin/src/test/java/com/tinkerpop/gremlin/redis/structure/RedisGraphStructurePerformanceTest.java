package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import com.tinkerpop.gremlin.redis.RedisGraphProvider;
import org.junit.runner.RunWith;

/**
 * Executes the Gremlin Structure Performance Test Suite using RedisGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructurePerformanceSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = RedisGraphProvider.class, graph = RedisGraph.class)
public class RedisGraphStructurePerformanceTest {

}
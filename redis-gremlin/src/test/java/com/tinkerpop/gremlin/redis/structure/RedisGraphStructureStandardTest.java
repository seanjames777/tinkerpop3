package com.tinkerpop.gremlin.redis.structure;

import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import com.tinkerpop.gremlin.redis.RedisGraphProvider;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using RedisGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = RedisGraphProvider.class, graph = RedisGraph.class)
public class RedisGraphStructureStandardTest {

}

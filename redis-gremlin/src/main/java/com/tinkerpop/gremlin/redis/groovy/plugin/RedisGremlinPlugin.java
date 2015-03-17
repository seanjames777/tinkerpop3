package com.tinkerpop.gremlin.redis.groovy.plugin;

import com.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import com.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import com.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import com.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;
import com.tinkerpop.gremlin.redis.structure.RedisGraph;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RedisGremlinPlugin extends AbstractGremlinPlugin {


    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + RedisGraph.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.redis";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) throws IllegalEnvironmentException, PluginInitializationException {

    }
}

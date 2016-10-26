package org.kairosdb.plugin.influx;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.kairosdb.plugin.influx.udp.InfluxUDPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class InfluxServerModule extends AbstractModule {
    public static final Logger logger = LoggerFactory.getLogger(InfluxServerModule.class);
    private Properties properties;

    public InfluxServerModule(Properties props) {
        this.properties = props;
    }

    @Override
    protected void configure() {
        logger.info("Configuring module InfluxServerModule");

        bind(InfluxUDPServer.class).in(Singleton.class);
    }
}

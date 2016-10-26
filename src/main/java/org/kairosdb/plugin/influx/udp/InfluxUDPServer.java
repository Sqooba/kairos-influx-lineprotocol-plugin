package org.kairosdb.plugin.influx.udp;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.plugin.influx.InfluxLineProtocolParser;
import org.kairosdb.plugin.influx.InfluxMetric;
import org.kairosdb.util.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Executors;

public class InfluxUDPServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory,
        KairosDBService {
    public static final Logger logger = LoggerFactory.getLogger(InfluxUDPServer.class);
    private static final String DEFAULT_SEPARATOR = ".";

    private final int port;
    private InetAddress address;
    private final KairosDatastore datastore;
    private final String separator;
    private final int maxSize;
    private ConnectionlessBootstrap bootstrap;

    @Inject
    private LongDataPointFactory longDataPointFactory = new LongDataPointFactoryImpl();

    @Inject
    private DoubleDataPointFactory doubleDataPointFactory = new DoubleDataPointFactoryImpl();

    private final InfluxLineProtocolParser influxLineProtocolParser = new InfluxLineProtocolParser();

    private final ChannelBuffer[] lineProtocolDelimiter = {ChannelBuffers.wrappedBuffer(new byte[]{'\n'})};

    public InfluxUDPServer(KairosDatastore datastore, @Named("kairosdb.influx.lineprotocol.port") int port,
                           @Named("kairosdb.influx.lineprotocol.separator") String separator,
                           @Named("kairosdb.influx.lineprotocol.max_size") int maxSize) {
        this(datastore, port, null, separator, maxSize);
    }

    @Inject
    public InfluxUDPServer(KairosDatastore datastore, @Named("kairosdb.influx.lineprotocol.port") int port,
                           @Named("kairosdb.influx.lineprotocol.address") String address,
                           @Named("kairosdb.influx.lineprotocol.separator") String separator,
                           @Named("kairosdb.influx.lineprotocol.max_size") int maxSize) {
        this.port = port;
        this.datastore = datastore;
        this.address = null;
        this.separator = separator;
        this.maxSize = maxSize;
        try {
            this.address = InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            logger.error("Unknown host name " + address + ", will bind to 0.0.0.0");
        }
    }

    //	@Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        // Add the text line codec combination first,
        DelimiterBasedFrameDecoder frameDecoder = new DelimiterBasedFrameDecoder(
                maxSize, lineProtocolDelimiter);
        frameDecoder.setMaxCumulationBufferComponents(maxSize);

        pipeline.addLast("framer", frameDecoder);
        pipeline.addLast("decoder", new StringDecoder());
        pipeline.addLast("encoder", new StringEncoder());

        // and then business logic.
        pipeline.addLast("handler", this);

        return pipeline;
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx,
                                final MessageEvent msgevent) {
        final Object message = msgevent.getMessage();
        if (message instanceof String) {
            try {
                String msgString = (String) message;

                InfluxMetric influxMetric = influxLineProtocolParser.parse(msgString);

                if (influxMetric == null) {
                    return;
                }

                if (StringUtils.isEmpty(influxMetric.getName())) {
                    logger.warn("Metric " + msgString + " don't have a name (measurement in Influx wording)");
                    return;
                }

                Map<String, DataPoint> dataPoints = influxMetric.getDataPoints();

                if (dataPoints.isEmpty()) {
                    logger.warn("Metric " + msgString + " is missing a don't have datapoint(s) (field in Influx wording)");
                    return;
                }

                final ImmutableSortedMap<String, String> tags = influxMetric.getTags();
                for (Map.Entry<String, DataPoint> entry : influxMetric.getDataPoints().entrySet()) {
                    datastore.putDataPoint(influxMetric.getName() + separator + entry.getKey(), tags, entry.getValue());
                }
            } catch (Exception e) {
                logger.error("Influx Line protocol error with line: \"{}\"", message, e);
            }
        } else {
            log("Invalid message. Must be of type String.");
        }
    }

    private static void log(String message) {
        log(message, null);
    }

    private static void log(String message, Exception e) {
        if (logger.isDebugEnabled())
            if (e != null)
                logger.debug(message, e);
            else
                logger.debug(message);
        else {
            if (e instanceof ValidationException)
                message = message + " Reason: " + e.getMessage();
            logger.warn(message);
        }
    }

    //	@Override
    public void start() throws KairosDBException {

        // Configure the server.
        bootstrap = new ConnectionlessBootstrap(
                new NioDatagramChannelFactory(
                        Executors.newCachedThreadPool()));

        // Configure the pipeline factory.
        bootstrap.setPipelineFactory(this);
//        serverBootstrap.setOption("child.tcpNoDelay", true);
//        serverBootstrap.setOption("child.keepAlive", true);
//        bootstrap.setOption("reuseAddress", true);

        bootstrap.setOption("broadcast", "false");

        bootstrap.setOption("sendBufferSize", maxSize);
        bootstrap.setOption("receiveBufferSize", maxSize);

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(address, port));
    }

    //	@Override
    public void stop() {
        if (bootstrap != null) {
            bootstrap.shutdown();
        }
    }
}
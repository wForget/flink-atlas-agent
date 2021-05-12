package cn.wangz.flink.atlas.agent.embedded;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import scala.Option;
import scala.collection.mutable.Buffer;

public class EmbeddedKafkaServer {
    public static final Logger LOG = LoggerFactory.getLogger(EmbeddedKafkaServer.class);

    public  static final String PROPERTY_PREFIX   = "atlas.kafka";
    private static final String KAFKA_DATA  = "data";
    public  static final String PROPERTY_EMBEDDED = "atlas.notification.embedded";

    private final Properties properties;
    private KafkaServer kafkaServer;
    private ServerCnxnFactory factory;


    public EmbeddedKafkaServer(Properties properties) {
        this.properties = properties;
    }

    public void start() {
        LOG.info("==> EmbeddedKafkaServer.start...");

        try {
            startZk();
            startKafka();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start embedded kafka", e);
        }
        LOG.info("<== EmbeddedKafkaServer.start...");
    }

    public void stop() {
        LOG.info("==> EmbeddedKafkaServer.stop...");

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (factory != null) {
            factory.shutdown();
        }

        LOG.info("<== EmbeddedKafka.stop...");
    }

    private String startZk() throws IOException, InterruptedException, URISyntaxException {
        String zkValue = properties.getProperty("zookeeper.connect");

        LOG.info("Starting zookeeper at {}", zkValue);

        URL zkAddress    = getURL(zkValue);
        File snapshotDir = constructDir("zk/txn");
        File logDir      = constructDir("zk/snap");

        factory = NIOServerCnxnFactory
                .createFactory(new InetSocketAddress(zkAddress.getHost(), zkAddress.getPort()), 1024);

        factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));

        String ret = factory.getLocalAddress().getAddress().toString();

        LOG.info("Embedded zookeeper for Kafka started at {}", ret);

        return ret;
    }

    private void startKafka() throws IOException, URISyntaxException {
        String kafkaValue = properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        LOG.info("Starting kafka at {}", kafkaValue);

        URL        kafkaAddress = getURL(kafkaValue);
        Properties brokerConfig = properties;

        brokerConfig.setProperty("broker.id", "1");
        brokerConfig.setProperty("host.name", kafkaAddress.getHost());
        brokerConfig.setProperty("port", String.valueOf(kafkaAddress.getPort()));
        brokerConfig.setProperty("log.dirs", constructDir("kafka").getAbsolutePath());
        brokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));

        List<KafkaMetricsReporter> metrics          = new ArrayList<>();
        Buffer<KafkaMetricsReporter> metricsReporters = scala.collection.JavaConversions.asScalaBuffer(metrics);

        kafkaServer = new KafkaServer(
                KafkaConfig.fromProps(brokerConfig), new SystemTime(), Option.apply(this.getClass().getName()), metricsReporters);

        kafkaServer.startup();

        LOG.info("Embedded kafka server started with broker config {}", brokerConfig);
    }

    private File constructDir(String dirPrefix) {
        File file = new File(properties.getProperty(KAFKA_DATA), dirPrefix);

        if (!file.exists() && !file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }

        return file;
    }

    private URL getURL(String url) throws MalformedURLException {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            return new URL("http://" + url);
        }
    }

}


package com.adaptris.kafka.embedded;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.adaptris.core.PortManager;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

//From http://pannoniancoder.blogspot.co.uk/2014/08/embedded-kafka-and-zookeeper-for-unit.html
// And https://gist.github.com/vmarcinko/e4e58910bcb77dac16e9
class EmbeddedKafkaCluster {
  private static final int BASE_PORT = 4242;

  private final List<Integer> ports;
  private final String zkConnection;
  private final Properties baseProperties;

  private final String brokerList;

  private final List<KafkaServer> brokers;
  private final List<File> logDirs;

  public EmbeddedKafkaCluster(String zkConnection) {
    this(zkConnection, new Properties());
  }

  public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties) {
    this(zkConnection, baseProperties, 1);
  }

  public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties, int members) {
    this.zkConnection = zkConnection;
    this.ports = resolvePorts(members);
    this.baseProperties = baseProperties;

    this.brokers = new ArrayList<KafkaServer>();
    this.logDirs = new ArrayList<File>();

    this.brokerList = constructBrokerList(this.ports);
  }

  private List<Integer> resolvePorts(int portCount) {
    List<Integer> resolvedPorts = new ArrayList<Integer>();
    for (int i = 0; i < portCount; i++) {
      resolvedPorts.add(PortManager.nextUnusedPort(BASE_PORT));
    }
    return resolvedPorts;
  }

  private String constructBrokerList(List<Integer> ports) {
    StringBuilder sb = new StringBuilder();
    for (Integer port : ports) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append("localhost:").append(port);
    }
    return sb.toString();
  }

  public void startup() {
    for (int i = 0; i < ports.size(); i++) {
      Integer port = ports.get(i);
      File logDir = FileHelper.createTempDir("kafka-local", this);

      Properties properties = new Properties();
      properties.putAll(baseProperties);
      properties.setProperty("zookeeper.connect", zkConnection);
      properties.setProperty("broker.id", String.valueOf(i + 1));
      properties.setProperty("host.name", "localhost");
      properties.setProperty("port", Integer.toString(port));
      properties.setProperty("log.dir", logDir.getAbsolutePath());
      // properties.setProperty("log.flush.interval.messages", String.valueOf(1));
      properties.setProperty("log.flush.offset.checkpoint.interval.ms", "100");
      KafkaServer broker = startBroker(properties);

      brokers.add(broker);
      logDirs.add(logDir);
    }
  }


  private KafkaServer startBroker(Properties props) {
    KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime(), null);
    server.startup();
    return server;
  }

  public Properties getProps() {
    Properties props = new Properties();
    props.putAll(baseProperties);
    props.put("metadata.broker.list", brokerList);
    props.put("zookeeper.connect", zkConnection);
    return props;
  }

  public String getBrokerList() {
    return brokerList;
  }

  public List<Integer> getPorts() {
    return ports;
  }

  public String getZkConnection() {
    return zkConnection;
  }

  public void shutdown() {
    for (KafkaServer broker : brokers) {
      try {
        broker.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    for (int p : ports) {
      PortManager.release(p);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
    sb.append("brokerList='").append(brokerList).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
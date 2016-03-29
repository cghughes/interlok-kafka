package com.adaptris.kafka.embedded;

import java.io.IOException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

public class KafkaServerWrapper {

  private static final int DEFAULT_TIMEOUT = 10000;
  private static final int DEFAULT_PARTITIONS = 10;
  private static final int DEFAULT_REPL_FACTOR = 1;

  private transient EmbeddedZookeeper zookeeper;
  private transient EmbeddedKafkaCluster cluster;

  public KafkaServerWrapper(int clusterMemberCount) {
    zookeeper = new EmbeddedZookeeper();
    cluster = new EmbeddedKafkaCluster(zookeeper.getConnection(), new Properties(), clusterMemberCount);
  }

  public String getConnections() {
    return cluster.getBrokerList();
  }

  public void start() throws IOException {
    zookeeper.startup();
    System.out.println("### Embedded Zookeeper connection: " + zookeeper.getConnection());
    cluster.startup();
    System.out.println("### Embedded Kafka cluster broker list: " + cluster.getBrokerList());
  }

  public void createTopic(String topic) throws Exception {
    ZkConnection zkConnection = new ZkConnection(zookeeper.getConnection(), DEFAULT_TIMEOUT);
    ZkClient zkClient = new ZkClient(zkConnection, DEFAULT_TIMEOUT);
    ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    AdminUtils.createTopic(zkUtils, topic, DEFAULT_PARTITIONS, DEFAULT_REPL_FACTOR, new Properties());
    zkUtils.close();
    zkClient.close();
    zkConnection.close();
  }

  public void shutdown() {
    cluster.shutdown();
    zookeeper.shutdown();
    zookeeper = null;
    cluster = null;
  }


  public static void main(String[] argv) throws Exception {
    KafkaServerWrapper wrapper = new KafkaServerWrapper(2);
    wrapper.start();
    Thread.sleep(5000);
    wrapper.shutdown();
    Thread.sleep(5000);
    System.gc();
  }
}

package com.adaptris.kafka.embedded;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.adaptris.core.PortManager;

// From http://pannoniancoder.blogspot.co.uk/2014/08/embedded-kafka-and-zookeeper-for-unit.html
// And https://gist.github.com/vmarcinko/e4e58910bcb77dac16e9
class EmbeddedZookeeper {
  private static final int BASE_PORT = 2181;

  private int port = -1;
  private int tickTime = 100;

  private ServerCnxnFactory factory;
  private File snapshotDir;
  private File logDir;

  public EmbeddedZookeeper() {
    this.port = PortManager.nextUnusedPort(BASE_PORT);
  }

  public void startup() throws IOException {
    this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024);
    this.snapshotDir = FileHelper.createTempDir("zk-snapshot", this);
    this.logDir = FileHelper.createTempDir("zk-log", this);

    try {
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }


  public void shutdown() {
    factory.shutdown();
    PortManager.release(port);
  }

  public String getConnection() {
    return "localhost:" + port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }

  public int getPort() {
    return port;
  }

  public int getTickTime() {
    return tickTime;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
    sb.append("connection=").append(getConnection());
    sb.append('}');
    return sb.toString();
  }
}

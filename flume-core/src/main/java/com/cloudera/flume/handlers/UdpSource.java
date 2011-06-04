package com.cloudera.flume.handlers;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.cloudera.util.ByteBufferInputStream;

public class UdpSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(UdpSource.class);
  final public static int UDP_PORT = 7831;
  int port = UDP_PORT;
  int maxsize = 1 << 15;
  DatagramSocket sock;
  public UdpSource() {
  }

  public UdpSource(int port) {
    this.port = port;
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing UdpSource on port " + port);
    if (sock == null) {
      LOG.warn("double close of UdpSocket on udp:" + port
          + " , (this is ok but odd)");
      return;
    }

    sock.close();
  }

  @Override
  public Event next() throws IOException {
    byte[] buf = new byte[maxsize];
    DatagramPacket pkt = new DatagramPacket(buf, maxsize);
    Event e = null;
    do {
      sock.receive(pkt);
      if (pkt.getLength() > 0) {
        byte[] message = new byte[pkt.getLength()];
        System.arraycopy(pkt.getData(), 0, message, 0, pkt.getLength());
        e = new EventImpl(message);
      }
    } while (e == null);

    LOG.info("new udp event");
    
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    sock = new DatagramSocket(port);
  }
  
  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(Context ctx, String... argv) {
        int port = UDP_PORT; // default udp port, need root permissions
        // for this.
        if (argv.length > 1) {
          throw new IllegalArgumentException("usage: udp([port no]) ");
        }

        if (argv.length == 1) {
          port = Integer.parseInt(argv[0]);
        }

        return new UdpSource(port);
      }

    };
  }
}

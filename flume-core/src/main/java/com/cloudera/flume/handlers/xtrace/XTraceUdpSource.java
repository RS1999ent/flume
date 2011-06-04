package com.cloudera.flume.handlers.xtrace;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.cloudera.util.ByteBufferInputStream;

public class XTraceUdpSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(XTraceUdpSource.class);
  final public static int UDP_PORT = 7831;
  int port = UDP_PORT;
  int maxsize = 1 << 15;
  DatagramSocket sock;
  public XTraceUdpSource() {
  }

  public XTraceUdpSource(int port) {
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
        String s = new String(message, 4, message.length - 4); 
        e = new EventImpl();
        Scanner sc = new Scanner(s);
        sc.nextLine();
        String line = sc.nextLine();
        String jobId = "0";
        String clientId = "0";
        String parentList = "";
        String timeStamp = "";
        String taskId = "";
        String reportId = "";
        while(sc.hasNextLine()) {
          String[] pair = line.split(": ");
          if (pair[0].equals("X-Trace")) {
            char[] value = pair[1].toCharArray();
            taskId = new String(value, 2, 16);
            reportId = new String(value, 18, 16);
            e.set("TaskID", taskId.getBytes());
            e.set("ReportID", taskId.getBytes());
          } else if (pair[0].equals("Edge")) {
            parentList += pair[1] + ",";
          } else if (pair[0].equals("Timestamp")) {
            timeStamp = pair[1];
            e.set("Timestamp", pair[1].getBytes());
          } else {
            e.set(pair[0], pair[1].getBytes());
          }
        }
        e.set("Edge", parentList.getBytes());
        String rowkey = jobId + "|" + clientId + "|" + taskId + "|" + parentList + reportId + "|" + timeStamp;
        e.set("rowkey", rowkey.getBytes());
      }
    } while (e == null);

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

        return new XTraceUdpSource(port);
      }

    };
  }
}

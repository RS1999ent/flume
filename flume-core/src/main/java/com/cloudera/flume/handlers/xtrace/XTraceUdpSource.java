package com.cloudera.flume.handlers.xtrace;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;

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
  int count = 0;
  private LinkedBlockingQueue<String> queue;
  private PacketReceiver receiver;
  public XTraceUdpSource() {
  }

  public XTraceUdpSource(int port) {
    this.port = port;
  }
  
  public static String[] idExtraction(String md) {
	  if (md.length() < 18) return new String[] {"00000000", "00000000"};
	   
	  int opidlength = 4;
	  int taskidlength = 4;
	   
	  byte flag = (byte) Integer.parseInt(md.substring(0, 2), 16);
	  if ((flag & 0x08) != 0) {
	   opidlength = 8;
	  }
	   
	  switch (flag & 0x03) {
	  case 0x00: taskidlength = 4; break;
	  case 0x01: taskidlength = 8; break;
	  case 0x02: taskidlength = 12; break;
	  case 0x03: taskidlength = 20; break;
	  default: // can't happen
	  }
	   
	  return new String[] {md.substring(2, 2 + 2*taskidlength), md.substring(2 + 2*taskidlength, 2 + 2*taskidlength + 2*opidlength)};
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing UdpSource on port " + port);
    if (sock == null) {
      LOG.warn("double close of UdpSocket on udp:" + port
          + " , (this is ok but odd)");
      return;
    }
    receiver.isRunning = false;
    while(true) {
      try {
        receiver.join();
        break;
      } catch (InterruptedException e) {
      }
    }
    sock.close();
    queue.clear();
    queue = null;
  }

  @Override
  public Event next() throws IOException {
    Event e = null;
    do {
      String s = null;
      while (true) {
        try {
          s = queue.take();
          break;
        } catch (InterruptedException ex) {
        }
      }
      //System.out.println(s);
      e = new EventImpl();
      Scanner sc = new Scanner(s);
      sc.nextLine();
      //String jobId = "0";
      //String clientId = "0";
      String tId = "0";
      String parentList = "";
      String timeStamp = "";
      String taskId = "";
      String reportId = "";
      do {
        String line = sc.nextLine();
        //System.out.println(line);
        String[] pair = line.split(":");
        pair[0] = pair[0].trim();
        pair[1] = pair[1].trim();
        //LOG.info(pair[0] + "," + pair[1]);
        if (pair[0].equals("X-Trace")) {
          if (pair[1].length() < 34) {
            LOG.warn("Metadata length is less than expected: " + pair[1].length());
            //LOG.warn("Report: " + s);
            //e = null;
            //break;
          }
          String[] ids = idExtraction(pair[1]);
          taskId = ids[0];
          reportId = ids[1];
          e.set("TaskID", taskId.getBytes("UTF-8"));
          e.set("ReportID", reportId.getBytes("UTF-8"));
        } else if (pair[0].equals("Edge")) {
          if (parentList.length() == 0)
            parentList += pair[1];
          else
            parentList += "," + pair[1];
        } else if (pair[0].equals("Timestamp")) {
          timeStamp = pair[1];
          e.set("Timestamp", pair[1].getBytes("UTF-8"));
        } else if (pair[0].equals("Label")) {
          e.set("inst_pt_name", pair[1].getBytes("UTF-8"));
          e.set("value", "1".getBytes("UTF-8"));
        } else if (pair[0].equals("TaskID")) {
          /*if (pair[1].equals("UNKNOWN")) {
            LOG.warn("Received trace from unknown place");
          }*/
          tId = pair[1];
          /*else if (pair[1].indexOf("_") == -1) {
            jobId = pair[1];
          } else {
            String[] parts = pair[1].split("_");
            if (parts.length <= 3) {
              jobId = pair[1];
            } else {
              jobId = parts[1] + "_" + parts[2];
              clientId = parts[3];
              for (int i = 4; i < parts.length; i++)
                clientId += "_" + parts[i];
            }
          }*/
        } else {
          e.set(pair[0], pair[1].getBytes("UTF-8"));
        }
      } while (sc.hasNextLine());
      if (e == null)
        continue;
      e.set("Edge", parentList.getBytes("UTF-8"));
      //String rowkey = jobId + "|" + clientId + "|" + taskId + "|" + (parentList.equals("") ? "" : parentList + ",") + reportId + "|" + timeStamp;
      String rowkey = tId + "|" + taskId + "|" + (parentList.equals("") ? "" : parentList + ",") + reportId + "|" + timeStamp;
      e.set("rowkey", rowkey.getBytes("UTF-8"));
    } while (e == null);
    count++;
    System.out.println("Received " + count);
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void open() throws IOException {
    queue = new LinkedBlockingQueue<String>();
    sock = new DatagramSocket(port);
    sock.setSoTimeout(1);
    receiver = new PacketReceiver();
    receiver.start();
  }
  
  public static SourceBuilder builder() {
    return new SourceBuilder() {

      @Override
      public EventSource build(Context ctx, String... argv) {
        int port = UDP_PORT; // default udp port, need root permissions
        // for this.
        if (argv.length > 1) {
          throw new IllegalArgumentException("usage: xtraceUdp([port no]) ");
        }

        if (argv.length == 1) {
          port = Integer.parseInt(argv[0]);
        }

        return new XTraceUdpSource(port);
      }

    };
  }

  private class PacketReceiver extends Thread {
    public boolean isRunning = true;
    @Override
    public void run() {
      byte[] buf = new byte[maxsize];
      DatagramPacket pkt = new DatagramPacket(buf, maxsize);
      while (isRunning) {
        try {
          sock.receive(pkt);
          String report = new String(pkt.getData(), 4, pkt.getLength() - 4);
          while(true) {
            try {
              queue.put(report);
              break;
            } catch (InterruptedException e) {

            }
          }
        } catch (IOException e) {
        }
      }
    }
  }
}

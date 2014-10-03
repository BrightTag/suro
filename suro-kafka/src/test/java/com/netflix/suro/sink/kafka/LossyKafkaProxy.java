package com.netflix.suro.sink.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Simple Kafka proxy that simulates network failures. Adapted from
 * http://www.java2s.com/Code/Java/Network-Protocol/Asimpleproxyserver.htm
 * 
 * This is more than just a simple UDP proxy; we need to rewrite metadata request packets
 * to replace destinationPort with listenPort.
 */
public class LossyKafkaProxy extends Thread {
    private int listenPort;
    private String destinationHost;
    private int destinationPort;
    private boolean willShutdown = false;
    private ServerSocket serverSocket;

    /** @param destinationHost must exactly match how Kafka identifies itself, which was localhost.localdomain on my machine. */
    public LossyKafkaProxy(int listenPort, String destinationHost, int destinationPort) throws IOException {
        this.listenPort = listenPort;
        this.destinationHost = destinationHost;
        this.destinationPort = destinationPort;
    }


    public int getListenPort(){
        return this.listenPort;
    }


    /** Returns immediately without waiting for thread to finish. */ 
    public void shutdown() throws InterruptedException{
        willShutdown = true;
    }


    @Override
    public void run() {
        final byte[] request = new byte[4096];
        byte[] reply = new byte[4096];
        // accept requests in a loop
        try {
            serverSocket = new ServerSocket(this.listenPort);
        } catch (IOException e) {
            System.err.println(e);
            return;
        }
        while( !willShutdown ){
            Socket client = null, server = null;
            try {
                // Wait for a connection on the local port
                client = serverSocket.accept();

                final InputStream streamFromClient = client.getInputStream();
                final OutputStream streamToClient = client.getOutputStream();

                // Make a connection to the real server.
                // If we cannot connect to the server, send an error to the
                // client, disconnect, and continue waiting for connections.
                try {
                    server = new Socket(destinationHost, destinationPort);
                } catch (IOException e) {
                    System.err.println("Proxy server cannot connect to " + destinationHost + ":"
                                       + destinationPort + ":\n" + e);
                    client.close();
                    continue;
                }

                // Get server streams.
                final InputStream streamFromServer = server.getInputStream();
                final OutputStream streamToServer = server.getOutputStream();

                // a thread to read the client's requests and pass them
                // to the server. A separate thread for asynchronous.
                Thread t = new Thread() {
                    public void run() {
                        int bytesRead;
                        try {
                            while ((bytesRead = streamFromClient.read(request)) != -1) {
                                streamToServer.write(request, 0, bytesRead);
                                streamToServer.flush();
                            }
                        } catch (IOException e) {
                            System.err.println("Failed to relay request: " + e);
                        }

                        // the client closed the connection to us, so close our
                        // connection to the server.
                        try {
                            streamToServer.close();
                        } catch (IOException e) {
                        }
                    }
                };

                // Start the client-to-server request thread running
                t.start();

                // Read the server's responses
                // and pass them back to the client.
                int bytesRead;
                try {
                    // read response from server
                    while ((bytesRead = streamFromServer.read(reply)) != -1);
                    // Modify response: replace [ASCII destinationHost][UInt32 destinationPort]
                    // with [ASCII destinationHost][UInt32 listenPort], wherever it occurs.
                    // !!!: Our goal is to modify just metadata responses naming leaders, but we
                    // may accidentally rewrite data by doing this "dumb" replace.
                    int matchIndex = indexOf(reply, this.destinationHost.getBytes());
                    if( matchIndex >= 0 ){
                        // change the Int32 after the hostname
                        int modifyIndex = matchIndex + this.destinationHost.length();
                        byte[] newPortBytes = ByteBuffer.allocate(4)
                                                        .order(ByteOrder.LITTLE_ENDIAN)
                                                        .putInt(this.listenPort).array();
                        for(int i=0; i<4; i++) reply[modifyIndex+i] = newPortBytes[i];
                    }

                    // write modified response back to client
                    streamToClient.write(reply, 0, bytesRead);
                    streamToClient.flush();
                } catch (IOException e) {
                    System.err.println("Failed to relay response: " + e);
                }

                // The server closed its connection to us, so we close our
                // connection to our client.
                streamToClient.close();
            } catch (IOException e) {
                System.err.println(e);
            } finally {
                try {
                    if (server != null) server.close();
                    if (client != null) client.close();
                } catch (IOException e) {
                }
            }
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
        }
    }


    /**
     * from http://stackoverflow.com/a/1507813
     * Knuth-Morris-Pratt Algorithm for Pattern Matching
     *
     * Finds the first occurrence of the pattern in the text.
     * @return -1 on failure
     */
    private static int indexOf(byte[] data, byte[] pattern) {
        int[] failure = computeFailure(pattern);

        int j = 0;
        if (data.length == 0) return -1;

        for (int i = 0; i < data.length; i++) {
            while (j > 0 && pattern[j] != data[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == data[i]) { j++; }
            if (j == pattern.length) {
                return i - pattern.length + 1;
            }
        }
        return -1;
    }

    /**
     * Computes the failure function using a boot-strapping process,
     * where the pattern is matched against itself.
     */
    private static int[] computeFailure(byte[] pattern) {
        int[] failure = new int[pattern.length];

        int j = 0;
        for (int i = 1; i < pattern.length; i++) {
            while (j > 0 && pattern[j] != pattern[i]) {
                j = failure[j - 1];
            }
            if (pattern[j] == pattern[i]) {
                j++;
            }
            failure[i] = j;
        }

        return failure;
    }
}

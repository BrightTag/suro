package com.netflix.suro.sink.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Simple UDP proxy that simulates network failures. Adapted from
 * http://www.java2s.com/Code/Java/Network-Protocol/Asimpleproxyserver.htm
 */
public class LossyProxy extends Thread {
    private int listenPort;
    private String destinationHost;
    private int destinationPort;
    ServerSocket serverSocket;

    public LossyProxy(int listenPort, String destinationHost, int destinationPort) throws IOException {
        this.listenPort = listenPort;
        this.destinationHost = destinationHost;
        this.destinationPort = destinationPort;
    }


    public int getListenPort(){
        return this.listenPort;
    }


    @Override
    public void run() {
        final byte[] request = new byte[4096];
        byte[] reply = new byte[4096];
        // accept requests in a loop
        while (true) {
            Socket client = null, server = null;
            try {
                // Wait for a connection on the local port
                serverSocket = new ServerSocket(this.listenPort);
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
                    while ((bytesRead = streamFromServer.read(reply)) != -1) {
                        streamToClient.write(reply, 0, bytesRead);
                        streamToClient.flush();
                    }
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
                    if (server != null)
                        server.close();
                    if (client != null)
                        client.close();
                } catch (IOException e) {
                }
            }
        }
    }
}

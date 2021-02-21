package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public class NioConnectionPool implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPool.class);

    private final InetSocketAddress serverAddress;
    private final Queue<NioChannel> pool;
    private final int connectionTimeout;

    public NioConnectionPool() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port));
    }

    public NioConnectionPool(InetSocketAddress address) {
        this(address, PocSettings.clientConnectionPoolSize, 10_000);
    }

    public NioConnectionPool(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.serverAddress = address;
        this.pool = new LinkedBlockingDeque<>(capacity);
        this.connectionTimeout = connectionTimeout;
    }

    public NioChannel getOrCreate() throws IOException {
        NioChannel result = pool.poll();
        if (result == null) {
            logger.debug("No pooled resource - creating a new one");
            result = new NioChannel(createChannel());
        }
        return result;
    }

    public void release(NioChannel connection) {
        if (!pool.offer(connection)) {
            logger.debug("Resource pool is full - closing released resource");
            closeSilently(connection);
        }
    }

    @Override
    public void close() {
        while (!pool.isEmpty()) {
            NioChannel connection = pool.poll();
            if (connection != null)
                closeSilently(connection);
        }
    }

    private SocketChannel createChannel() throws IOException {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(true);
        //LRMIUtilities.initNewSocketProperties(socketChannel);
        socketChannel.socket().connect(serverAddress, connectionTimeout);
        return socketChannel;
    }

    private void closeSilently(NioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
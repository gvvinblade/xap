package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.ServerAddress;
import com.gigaspaces.lrmi.nio.ByteBufferPacketSerializer;
import com.gigaspaces.lrmi.nio.IPacket;
import com.gigaspaces.lrmi.nio.LrmiChannel;
import com.gigaspaces.lrmi.nio.ReplyPacket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.RDMA_CONNECT_TIMEOUT;

public class RdmaChannel extends LrmiChannel {

    private final GSRdmaEndpointFactory factory;
    private GSRdmaClientEndpoint endpoint;

    private RdmaChannel(RdmaWriter writer, RdmaReader reader, GSRdmaEndpointFactory factory, GSRdmaClientEndpoint endpoint) {
        super(writer, reader);
        this.factory = factory;
        this.endpoint = endpoint;
    }

    public static RdmaChannel create(ServerAddress address) throws IOException {
        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory(new LrmiRdmaResourceFactory(),RdmaChannel::deserializePacket);
        GSRdmaClientEndpoint endpoint = (GSRdmaClientEndpoint) factory.create();
        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(address.getHost());
        InetSocketAddress socketAddress = new InetSocketAddress(ipAddress, address.getPort());
        try {
            endpoint.connect(socketAddress, RDMA_CONNECT_TIMEOUT);
        } catch (Exception e) {
            throw new IOException(e);
        }

        RdmaWriter writer = new RdmaWriter();
        RdmaReader reader = new RdmaReader();
        return new RdmaChannel(writer, reader, factory, endpoint);
    }

    @Override
    public SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlocking() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSocketDesc() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSocketDisplayString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrSocketDisplayString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        try {
            factory.close();
            endpoint.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Object deserializePacket(ByteBuffer buffer){
        try {
            ByteBufferPacketSerializer serializer = new ByteBufferPacketSerializer(buffer);
            IPacket packet = new ReplyPacket<>();
            return serializer.deserialize(packet);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return e;
        }
    }

    public ClientTransport getTransport(){
        return endpoint.getTransport();
    }

}

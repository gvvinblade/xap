package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

public class GSRdmaClientEndpoint extends GSRdmaAbstractEndpoint {

    private final Function<ByteBuffer, Object> deserialize;
    private ClientTransport transport;
    private final RdmaResourceFactory factory;

    public GSRdmaClientEndpoint(RdmaActiveEndpointGroup<GSRdmaAbstractEndpoint> endpointGroup,
                                RdmaCmId idPriv, RdmaResourceFactory factory, Function<ByteBuffer, Object> deserialize) throws IOException {
        super(endpointGroup, idPriv, false);
        this.factory = factory;
        this.deserialize = deserialize;
    }

    //important: we override the init method to prepare some buffers (memory registration, post recv, etc).
    //This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
    public void init() throws IOException {
        super.init();
        factory.setEndpoint(this);
        transport = new ClientTransport(this, factory, deserialize);
    }

    public void dispatchCqEvent(IbvWC wc) throws IOException {
        DiSNILogger.getLogger().info("CLIENT: op code = " + IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) + ", id = " + wc.getWr_id());
        getTransport().onCompletionEvent(wc);
        DiSNILogger.getLogger().info("CLIENT-DONE: op code = " + IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) + ", id = " + wc.getWr_id());
    }


    public ClientTransport getTransport() {
        return transport;
    }

}

package com.gigaspaces.sql.datagateway.netty.server;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Pojo2 implements Externalizable {
    private String val;

    public Pojo2() {
    }

    public Pojo2(String val) {
        this.val = val;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(val);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        val = in.readUTF();
    }
}

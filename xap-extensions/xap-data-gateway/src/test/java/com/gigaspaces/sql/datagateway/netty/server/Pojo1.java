package com.gigaspaces.sql.datagateway.netty.server;

import com.gigaspaces.annotation.pojo.SpaceId;

public class Pojo1 {
    private String id;
    private Pojo2 pojo2;

    public Pojo1() {
    }

    public Pojo1(Pojo2 pojo2) {
        this.pojo2 = pojo2;
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Pojo2 getPojo2() {
        return pojo2;
    }

    public void setPojo2(Pojo2 pojo2) {
        this.pojo2 = pojo2;
    }
}

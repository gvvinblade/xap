package com.gigaspaces.sql.datagateway.netty.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RecordSerializerTest {

    private final RecordSerializer serializer = new RecordSerializer();

    static class Pojo1 {
        public String f1;
        private Pojo2 f2;
        public Object f3;
        private Integer f4;

        public Pojo2 getF2() {
            return f2;
        }

        public void setF2(Pojo2 f2) {
            this.f2 = f2;
        }

        public Integer getF4() {
            return f4;
        }
    }

    static class Pojo2 {
        public double f1;
        public Pojo3[] f2;
        public int[] f3;
    }

    static class Pojo3 {
        boolean f1;
    }

    @Test
    public void complexObjectSerialization() throws Exception {
        Pojo3 p3_1 = new Pojo3(), p3_2 = new Pojo3();
        p3_1.f1 = false;
        p3_2.f1 = true;
        Pojo2 p2 = new Pojo2();
        p2.f1 = 0.1;
        p2.f2 = new Pojo3[] {p3_1, p3_2};
        p2.f3 = new int[]{1,2,3};
        Pojo1 p1 = new Pojo1();
        p1.f1 = "field\"1\"Val\\";
        p1.f2 = p2;
        p1.f4 = 10;

        String s = serializer.asString(p1);
        Assertions.assertEquals("(\"field\\\"1\\\"Val\\\\\",(0.1,{(f),(t)},{1,2,3}),,10)", s);
    }
}
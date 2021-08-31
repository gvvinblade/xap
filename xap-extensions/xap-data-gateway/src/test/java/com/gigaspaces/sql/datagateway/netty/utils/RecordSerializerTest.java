package com.gigaspaces.sql.datagateway.netty.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RecordSerializerTest {

    private final RecordSerializer serializer = new RecordSerializer();

    static class Pojo1 {
        private String f1;
        private Pojo2 f2;
        private Object f3;
        private Integer f4;

        public String getF1() {
            return f1;
        }

        public void setF1(String f1) {
            this.f1 = f1;
        }

        public Pojo2 getF2() {
            return f2;
        }

        public void setF2(Pojo2 f2) {
            this.f2 = f2;
        }

        public Object getF3() {
            return f3;
        }

        public void setF3(Object f3) {
            this.f3 = f3;
        }

        public Integer getF4() {
            return f4;
        }

        public void setF4(Integer f4) {
            this.f4 = f4;
        }
    }

    static class Pojo2 {
        private double f1;
        private Pojo3[] f2;
        private int[] f3;

        public double getF1() {
            return f1;
        }

        public void setF1(double f1) {
            this.f1 = f1;
        }

        public Pojo3[] getF2() {
            return f2;
        }

        public void setF2(Pojo3[] f2) {
            this.f2 = f2;
        }

        public int[] getF3() {
            return f3;
        }

        public void setF3(int[] f3) {
            this.f3 = f3;
        }
    }

    static class Pojo3 {
        private boolean f1;

        public boolean isF1() {
            return f1;
        }

        public void setF1(boolean f1) {
            this.f1 = f1;
        }
    }

    static class Pojo4 extends Pojo3 {
        private int f2;

        public int getF2() {
            return f2;
        }

        public void setF2(int f2) {
            this.f2 = f2;
        }
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

    @Test
    public void inheritedObjectSerialization() throws Exception {
        Pojo4 p4 = new Pojo4();
        p4.setF1(true);
        p4.setF2(4);

        String s = serializer.asString(p4);
        Assertions.assertEquals("(4,t)", s);
    }
}
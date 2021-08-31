package com.gigaspaces.sql.datagateway.netty.utils;

import com.gigaspaces.sql.datagateway.netty.exception.NonBreakingException;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;
import com.gigaspaces.utils.Pair;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.linq4j.tree.Primitive;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RecordSerializer {
    private final Cache<Class<?>, Serializer> cache = CacheBuilder
        .newBuilder().softValues().maximumSize(1000).build();

    @FunctionalInterface
    interface Serializer {
        String asString(Object obj) throws Exception;
    }

    @FunctionalInterface
    interface PropertySerializer {
        String asString(Object obj) throws Exception;
    }

    @FunctionalInterface
    public interface IntF extends PropertyWrapper {
        int apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return int.class;
        }

        @Override
        default int getInt(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface LongF extends PropertyWrapper {
        long apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return long.class;
        }

        @Override
        default long getLong(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface ShortF extends PropertyWrapper {
        short apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return short.class;
        }

        @Override
        default short getShort(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface FloatF extends PropertyWrapper {
        float apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return float.class;
        }

        @Override
        default float getFloat(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface DoubleF extends PropertyWrapper {
        double apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return double.class;
        }

        @Override
        default double getDouble(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface BooleanF extends PropertyWrapper {
        boolean apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return boolean.class;
        }

        @Override
        default boolean getBoolean(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface CharF extends PropertyWrapper {
        char apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return char.class;
        }

        @Override
        default char getChar(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface ByteF extends PropertyWrapper {
        byte apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return byte.class;
        }

        @Override
        default byte getByte(Object o) throws Exception {
            return apply(o);
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    @FunctionalInterface
    public interface ObjectF extends PropertyWrapper {
        Object apply(Object o) throws Exception;

        @Override
        default Class<?> getType() {
            return Object.class;
        }

        @Override
        default Object get(Object o) throws Exception {
            return apply(o);
        }
    }

    private static class TypedWrapper implements ObjectF {
        private final Class<?> type;
        private final ObjectF delegate;

        public TypedWrapper(Class<?> type, ObjectF delegate) {
            this.type = type;
            this.delegate = delegate;
        }

        @Override
        public Class<?> getType() {
            return type;
        }

        @Override
        public Object apply(Object o) throws Exception {
            return delegate.apply(o);
        }
    }

    public interface PropertyWrapper {
        Class<?> getType();
        default int getInt(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default long getLong(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default double getDouble(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default float getFloat(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default char getChar(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default short getShort(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default boolean getBoolean(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default byte getByte(Object o) throws Exception { throw new UnsupportedOperationException(); }
        default Object get(Object o) throws Exception { throw new UnsupportedOperationException(); }
    }

    public String asString(Object obj) throws ProtocolException {
        if (obj == null)
            return "";
        try {
            return cache.get(obj.getClass(), () -> newSerializer(obj.getClass())).asString(obj);
        } catch (ProtocolException e) {
            throw e;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ProtocolException)
                throw (ProtocolException) e.getCause();
            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Cannot serialize object [" + obj + "]", e);
        } catch (Exception e) {
            throw new NonBreakingException(ErrorCodes.UNSUPPORTED_FEATURE, "Cannot serialize object [" + obj + "]", e);
        }
    }

    private Serializer newSerializer(Class<?> clazz) throws Exception {
        if (Primitive.isBox(clazz))
            return obj -> obj == null ? "" : obj.toString();
        if (clazz.isArray())
            return obj -> asArrayString(clazz, obj);

        Map<Pair<Class<?>, String>, PropertyWrapper> props = new LinkedHashMap<>();
        Class<?> clazz0 = clazz;
        do {
            for (Field f : clazz0.getDeclaredFields()) {
                props.put(new Pair<>(clazz0, f.getName()), null); // placeholder to get right fields order
            }
            for (Method m : clazz0.getDeclaredMethods()) { // access getters
                if (isGetterMethod(m)) {
                    props.replace(new Pair<>(clazz0, propertyName(m)), createWrapper(m));
                }
            }
        } while ((clazz0 = clazz0.getSuperclass()) != null);

        List<PropertySerializer> serializers = new ArrayList<>(props.size());
        for (PropertyWrapper pw : props.values()) {
            if (pw != null) {
                Class<?> type = pw.getType();
                if (Primitive.isBox(clazz)) {
                    serializers.add(o -> asSimpleString(pw, o));
                } else if (Primitive.is(type)) {
                    serializers.add(o -> asPrimitiveString(pw, o));
                } else if (String.class.isAssignableFrom(type)) {
                    serializers.add(o -> asEscapedString(pw, o));
                } else if (type.isArray()) {
                    serializers.add(o -> asArrayString(pw, o));
                } else {
                    serializers.add(o -> asString(pw.get(o)));
                }
            }
        }

        return obj -> {
            StringBuilder b = new StringBuilder().append('(');
            for (int i = 0; i < serializers.size(); i++) {
                if (i > 0) b.append(',');
                b.append(serializers.get(i).asString(obj));
            }
            return b.append(')').toString();
        };
    }

    private String asArrayString(PropertyWrapper f, Object o) throws Exception {
        return asArrayString(f.getType(), f.get(o));
    }

    private String asArrayString(Class<?> type, Object array0) throws Exception {
        if (type == int[].class) {
            int[] array = (int[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }
        if (type == long[].class) {
            long[] array = (long[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }
        if (type == byte[].class) {
            byte[] array = (byte[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }
        if (type == char[].class) {
            char[] array = (char[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }
        if (type == boolean[].class) {
            boolean[] array = (boolean[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i] ? 't' : 'f');
            }
            return b.append('}').toString();
        }
        if (type == short[].class) {
            short[] array = (short[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }
        if (type == double[].class) {
            double[] array = (double[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }
        if (type == float[].class) {
            float[] array = (float[]) array0;
            if (array == null)
                return "";
            if (array.length == 0)
                return "{}";

            StringBuilder b = new StringBuilder()
                .append('{').append(asString(array[0]));
            for (int i = 1; i < array.length; i++) {
                b.append(',').append(array[i]);
            }
            return b.append('}').toString();
        }

        Object[] array = (Object[]) array0;
        if (array == null)
            return "";
        if (array.length == 0)
            return "{}";

        StringBuilder b = new StringBuilder()
            .append('{').append(asString(array[0]));
        for (int i = 1; i < array.length; i++) {
            b.append(',').append(asString(array[i]));
        }
        return b.append('}').toString();
    }

    private static String asSimpleString(PropertyWrapper f, Object o) throws Exception {
        Object res = f.get(o);
        return res == null ? "" : res.toString();
    }

    private static String asEscapedString(PropertyWrapper f, Object o) throws Exception {
        String res = asSimpleString(f, o);
        if (res. isEmpty())
            return res;

        StringBuilder b = new StringBuilder().append('\"');
        for (int i = 0; i < res.length(); i++) {
            char c = res.charAt(i);
            switch (c) {
                case '\\':
                case '\"':
                    b.append('\\');
                default:
                    b.append(c);
            }
        }
        return b.append('\"').toString();
    }

    private static String asPrimitiveString(PropertyWrapper f, Object o) throws Exception {
        Class<?> type = f.getType();
        if (type == int.class)
            return Integer.toString(f.getInt(o));
        if (type == long.class)
            return Long.toString(f.getLong(o));
        if (type == byte.class)
            return Byte.toString(f.getByte(o));
        if (type == char.class)
            return Character.toString(f.getChar(o));
        if (type == boolean.class)
            return f.getBoolean(o) ? "t" : "f";
        if (type == short.class)
            return Short.toString(f.getShort(o));
        if (type == double.class)
            return Double.toString(f.getDouble(o));
        if (type == float.class)
            return Float.toString(f.getFloat(o));
        throw new UnsupportedOperationException("Unsupported field type: " + type);
    }

    private static boolean isGetterMethod(Method method) {
        String name = method.getName();
        return method.getParameterCount() == 0 &&
            !Modifier.isStatic(method.getModifiers()) &&
            !name.equals("get") &&
            !name.equals("is") &&
            !name.equals("getClass") &&
            (name.startsWith("get") || (name.startsWith("is") && method.getReturnType() == boolean.class));
    }

    private static String propertyName(Method getter) {
        String name = getter.getName();
        if (name.startsWith("get"))
            return Character.toLowerCase(name.charAt(3)) + name.substring(4);
        else if (name.startsWith("is"))
            return Character.toLowerCase(name.charAt(2)) + name.substring(3);
        throw new IllegalStateException("Unexpected method name: " + name);
    }

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    private static PropertyWrapper createWrapper(Method getter) throws Exception {
        try {
            PropertyWrapper wrapper = (PropertyWrapper) createCallSite(LOOKUP.unreflect(getter)).getTarget().invoke();
            return wrapper instanceof ObjectF
                ? new TypedWrapper(getter.getReturnType(), (ObjectF) wrapper)
                : wrapper;
        } catch (Throwable e) {
            throw new ExecutionException(e);
        }
    }

    private static CallSite createCallSite(MethodHandle getterMethodHandle) throws Exception {
        Class<?> type = getterMethodHandle.type().returnType();
        if (type == int.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(IntF.class),
                MethodType.methodType(int.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == long.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(LongF.class),
                MethodType.methodType(long.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == char.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(CharF.class),
                MethodType.methodType(char.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == short.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(ShortF.class),
                MethodType.methodType(short.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == double.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(DoubleF.class),
                MethodType.methodType(double.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == float.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(FloatF.class),
                MethodType.methodType(float.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == boolean.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(BooleanF.class),
                MethodType.methodType(boolean.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());
        if (type == byte.class)
            return LambdaMetafactory.metafactory(LOOKUP, "apply",
                MethodType.methodType(ByteF.class),
                MethodType.methodType(byte.class, Object.class),
                getterMethodHandle, getterMethodHandle.type());

        return LambdaMetafactory.metafactory(LOOKUP, "apply",
            MethodType.methodType(ObjectF.class),
            MethodType.methodType(Object.class, Object.class),
            getterMethodHandle, getterMethodHandle.type());
    }
}

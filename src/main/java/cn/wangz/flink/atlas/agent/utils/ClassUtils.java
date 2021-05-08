package cn.wangz.flink.atlas.agent.utils;

import java.lang.reflect.Field;

public class ClassUtils {

    public static <T> T getFiledValue(Object object, String filedName, Class<T> filedClass)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = object.getClass().getDeclaredField(filedName);
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        Object filedValue = field.get(object);
        field.setAccessible(accessible);
        return (T) filedValue;
    }

    public static void main(String[] args) {

    }

}

package com.hbase;

import java.lang.reflect.Field;

/**
 * @auther lf
 * @date 2017/9/6
 * @description 描述
 */
public class ClassTest {

    private static void toMapPut(Class tClass){
        Field[] declaredFields = tClass.getDeclaredFields();
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < declaredFields.length; i++) {
            stringBuilder.append("map.put(\"").append(declaredFields[i].getName()).append("\",\"\");\n");
        }
        System.out.println(stringBuilder);
    }

}

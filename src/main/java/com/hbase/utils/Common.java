package com.hbase.utils;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.parser.deserializer.JavaBeanDeserializer;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;
import com.alibaba.fastjson.serializer.JavaBeanSerializer;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.hbase.dao.impl.BaseDaoJdbcTemplateImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
import sun.reflect.misc.MethodUtil;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by lf on 2017-08-03 13:57:52.
 */
public class Common {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseDaoJdbcTemplateImpl.class);

    public static final Map<String, Object> EMPTY_MAP = new HashMap<>(0);

    // 方法缓存
    private static final ConcurrentMap<String, Method[]> METHOD_CACHE = new ConcurrentReferenceHashMap<>(64);

    // javaBean序列化缓存
    private static final LoadingCache<Object, JavaBeanSerializer> SERIALIZER_CACHE = CacheBuilder.newBuilder()
            .maximumSize(256)
            .softValues()
            .expireAfterAccess(1, TimeUnit.DAYS)
            .build(new CacheLoader<Object, JavaBeanSerializer>() {
                @Override
                public JavaBeanSerializer load(@Nonnull final Object key) throws Exception {
                    LOGGER.info("load cache ：{}",key.getClass().getName());
                    return new JavaBeanSerializer(key.getClass());
                }
            });

    public static String toString(Object obj) {
        return obj == null ? "" : obj.toString();
    }

    /**
     * 获取map中的字符串值
     * lf
     * 2017-5-25 16:46:45
     *
     * @param map
     * @param key
     * @return
     */
    public static String getMapString(Map<String, List<String>> map, String key) {
        if (CollectionUtils.isEmpty(map) ||
                StringUtils.isBlank(key) ||
                !map.containsKey(key)) return "";
        final List<String> list = map.get(key);
        if (CollectionUtils.isEmpty(list)) return "";
        return list.get(0);
    }

    /**
     * 判读对象是否为空
     * lf
     * 2017-5-25 16:41:28
     *
     * @param obj
     * @return
     */
    public static boolean isEmpty(Object obj) {
        return StringUtils.isBlank(toString(obj));
    }

    /**
     * 判读是否存在为空的对象
     * lf
     * 2017-5-25 16:41:28
     *
     * @param obj 对象
     * @return
     */
    public static boolean hasEmpty(Object... obj) {
        if (obj == null || obj.length == 0) return true;
        for (int i = 0; i < obj.length; i++) {
            if (isEmpty(obj[i])) return true;
        }
        return false;
    }

    /**
     * 是否是数字
     *
     * @param obj 对象
     * @return
     */
    public static boolean isNumber(Object obj) {
        return NumberUtils.isNumber(toString(obj));
    }

    /**
     * 获取int值
     * lf
     * 2017-5-25 16:43:52
     *
     * @param obj
     * @return
     */
    public static Integer getInteger(Object obj) {
        return !isNumber(obj) ? 0 : Integer.valueOf(obj.toString());
    }

    /**
     * 获取long值
     * lf
     * 2017-5-25 16:43:52
     *
     * @param obj
     * @return
     */
    public static Long getLong(Object obj) {
        return !isNumber(obj) ? 0L : Long.valueOf(obj.toString());
    }

    /**
     * 获取double值
     * lf
     * 2017-5-25 16:43:52
     *
     * @param obj
     * @return
     */
    public static double getDouble(Object obj) {
        return !isNumber(obj) ? 0.0 : Double.valueOf(obj.toString());
    }

    /**
     * 首字母小写
     *
     * @param str
     * @return
     */
    public static String unCapitalize(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return str;
        }
        return new StrBuilder(strLen)
                .append(Character.toLowerCase(str.charAt(0)))
                .append(str.substring(1))
                .toString();
    }

    /**
     * lf
     * 2017-08-17 11:43:57
     * 将对象转换为map
     *
     * @param obj        对象
     * @param nullRemove 是否清楚value为null的值
     * @return
     */
    public static Map<String, Object> toMap(Object obj, boolean nullRemove) {
        if (obj == null) return EMPTY_MAP;
        try {
            JavaBeanSerializer serializer = SERIALIZER_CACHE.get(obj);
            Map<String, Object> map = serializer.getFieldValuesMap(obj);
            if (map.isEmpty()) return EMPTY_MAP;
            if (nullRemove) {
                return Maps.filterValues(map, Objects::nonNull);
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("将对象转换为map失败！", e);
        }

    }

    /**
     * lf
     * 2017-08-17 11:43:57
     * 将对象转换为map，将value为null的清除掉
     *
     * @param obj 对象
     * @return
     */
    public static Map<String, Object> toMap(Object obj) {
        return toMap(obj, true);
    }

    /**
     * lf
     * 2017-08-17 11:43:57
     * 将对象转换为map，将value为null的清除掉
     *
     * @param map   map
     * @param clazz 类型
     * @return
     */
    public static <T> T toObject(Map<String, Object> map, Class<T> clazz) {
        JavaBeanDeserializer javaBeanDeser = null;
        final ParserConfig config = ParserConfig.getGlobalInstance();
        ObjectDeserializer deserizer = config.getDeserializer(clazz);
        if (deserizer instanceof JavaBeanDeserializer) {
            javaBeanDeser = (JavaBeanDeserializer) deserizer;
        }
        if (javaBeanDeser == null) {
            throw new JSONException("can not get javaBeanDeserializer. " + clazz.getName());
        }
        try {
            return (T) javaBeanDeser.createInstance(map, config);
        } catch (Exception e) {
            throw new RuntimeException("将map转换为对象失败！", e);
        }
    }

    /**
     * lf
     * 2017-08-17 11:43:57
     * 将对象转换为map，将value为null的清除掉
     *
     * @param map   map
     * @param clazz 类型
     * @return
     */
    public static <T> T toObject2(Map<String, Object> map, Class<T> clazz) {
        try {
            final T instance = clazz.newInstance();
            final Method[] methods = getMethod(clazz);
            Arrays.stream(methods).filter(method -> method.getName().startsWith("set")).forEach(method -> {
                String name = unCapitalize(method.getName().substring(3));
                if (map.containsKey(name)) {
                    try {
                        method.invoke(instance, map.get(name));
                    } catch (Exception e) {
                        LOGGER.error("使用反射将值设置失败: {}", name);
                    }
                }
            });
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("将map转化为object失败！", e);
        }
    }

    /**
     * 通过反射获取方法
     *
     * @param clazz 类
     * @return
     * @throws NoSuchMethodException
     */
    private static <T> Method[] getMethod(final Class<T> clazz) throws NoSuchMethodException {
        String name = clazz.getName();
        Method[] methods = METHOD_CACHE.get(name);
        if (methods != null) {
            return methods;
        } else {
            LOGGER.info("--------------refact------------------");
            methods = MethodUtil.getPublicMethods(clazz);
            METHOD_CACHE.put(name, methods);
            return methods;
        }
    }

    public static void main(String[] args) {
        System.out.println(hasEmpty(1, 2, 3));
    }
}

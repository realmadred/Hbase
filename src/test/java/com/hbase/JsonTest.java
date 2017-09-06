package com.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.JavaBeanSerializer;
import com.google.common.collect.Maps;
import com.hbase.test.entity.User;
import com.hbase.utils.Common;
import com.hbase.utils.Lpn;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import sun.reflect.misc.MethodUtil;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class JsonTest {

    @Test
    public void test1() {
        String json = "[{\"ProID\":7,\"name\":\"吉林省\"}, {\"ProID\":8,\"name\":\"黑龙江省\"}, {\"ProID\":9,\"name\":\"上海市\"}, {\"ProID\":10,\"name\":\"江苏省\"}, {\"ProID\":11,\"name\":\"浙江省\"}, {\"ProID\":12,\"name\":\"安徽省\"}, {\"ProID\":13,\"name\":\"福建省\"}, {\"ProID\":14,\"name\":\"江西省\"}, {\"ProID\":15,\"name\":\"山东省\"}, {\"ProID\":16,\"name\":\"河南省\"}, {\"ProID\":17,\"name\":\"湖北省\"}, {\"ProID\":18,\"name\":\"湖南省\"}, {\"ProID\":19,\"name\":\"广东省\"}, {\"ProID\":20,\"name\":\"海南省\"}, {\"ProID\":21,\"name\":\"广西壮族自治区\"}, {\"ProID\":22,\"name\":\"甘肃省\"}, {\"ProID\":23,\"name\":\"陕西省\"}, {\"ProID\":24,\"name\":\"新疆维吾尔自治区\"}, {\"ProID\":25,\"name\":\"青海省\"}, {\"ProID\":26,\"name\":\"宁夏回族自治区\"}, {\"ProID\":27,\"name\":\"重庆市\"}, {\"ProID\":28,\"name\":\"四川省\"}, {\"ProID\":29,\"name\":\"贵州省\"}, {\"ProID\":30,\"name\":\"云南省\"}, {\"ProID\":31,\"name\":\"西藏自治区\"}, {\"ProID\":32,\"name\":\"台湾省\"}, {\"ProID\":33,\"name\":\"澳门特别行政区\"}, {\"ProID\":34,\"name\":\"香港特别行政区\"}]";
        JSONArray objects = JSON.parseArray(json);
        objects.forEach(o -> {
            JSONObject jsonObject = JSON.parseObject(o.toString());
            jsonObject.put("name", provinceForShort(jsonObject.getString("name")));
            System.out.println("LPN_MAP.put(\"" + jsonObject.getString("ProID") + "\",\"" + provinceForShort(jsonObject.getString("name")) + "\");");
        });
    }

    String provinceForShort(String province) {
        if (province.equals("北京市"))
            return "京";
        else if (province.equals("天津市"))
            return "津";
        else if (province.equals("重庆市"))
            return "渝";
        else if (province.equals("上海市"))
            return "沪";
        else if (province.equals("河北省"))
            return "冀";
        else if (province.equals("山西省"))
            return "晋";
        else if (province.equals("辽宁省"))
            return "辽";
        else if (province.equals("吉林省"))
            return "吉";
        else if (province.equals("黑龙江省"))
            return "黑";
        else if (province.equals("江苏省"))
            return "苏";
        else if (province.equals("浙江省"))
            return "浙";
        else if (province.equals("安徽省"))
            return "皖";
        else if (province.equals("福建省"))
            return "闽";
        else if (province.equals("江西省"))
            return "赣";
        else if (province.equals("山东省"))
            return "鲁";
        else if (province.equals("河南省"))
            return "豫";
        else if (province.equals("湖北省"))
            return "鄂";
        else if (province.equals("湖南省"))
            return "湘";
        else if (province.equals("广东省"))
            return "粤";
        else if (province.equals("海南省"))
            return "琼";
        else if (province.equals("四川省"))
            return "川";
        else if (province.equals("贵州省"))
            return "贵";
        else if (province.equals("云南省"))
            return "云";
        else if (province.equals("陕西省"))
            return "陕";
        else if (province.equals("甘肃省"))
            return "甘";
        else if (province.equals("青海省"))
            return "青";
        else if (province.equals("台湾省"))
            return "台";
        else if (province.equals("内蒙古自治区"))
            return "蒙";
        else if (province.equals("广西壮族自治区"))
            return "桂";
        else if (province.equals("宁夏回族自治区"))
            return "宁";
        else if (province.equals("新疆维吾尔自治区 "))
            return "新";
        else if (province.equals("西藏自治区"))
            return "藏";
        else if (province.equals("香港特别行政区"))
            return "港";
        else if (province.equals("澳门特别行政区"))
            return "澳";
        else
            return province;

    }

    @Test
    public void test2() {
        System.out.println(Lpn.getProv("18"));
    }

    @Test
    public void test3() {
        System.out.println(Lpn.getCode("湘"));
    }

    @Test
    public void test4() {
        User user = init();
        JSONObject jsonObject = (JSONObject) JSON.toJSON(user);
        Iterator<Map.Entry<String, Object>> iterator = jsonObject.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            Object value = next.getValue();
            if (value == null) iterator.remove();
        }
        System.out.println(jsonObject);
    }

    @Test
    public void test5() throws Exception {
        User user = init();
        JavaBeanSerializer serializer = new JavaBeanSerializer(User.class);
        Map<String, Object> map = serializer.getFieldValuesMap(user);
        System.out.println(Maps.filterValues(map, Objects::nonNull));
    }

    @Test
    public void test6() throws Exception {
        User user = init();
        for (int i = 0; i < 200_0000 ; i++) {
            Common.toMap(user);
        }
    }

    @Test
    public void test7() throws Exception {
        User user = init();
        Map<String, Object> map = Common.toMap(user);
        User object;
        for (int i = 0; i < 1_000; i++) {
            object = Common.toObject(map, User.class);
            System.out.println(object);
        }
    }

    @NotNull
    private User init() {
        User user = new User();
        user.setName("zhangsan");
        user.setAge(22);
        user.setSex(1);
        user.setId(55);
        user.setStart(1);
        return user;
    }

    @Test
    public void test8() throws Exception {
        User user = init();
        Map<String, Object> map = Common.toMap(user);
        User object;
        for (int i = 0; i < 1000_0000; i++) {
            object = Common.toObject2(map, User.class);
            System.out.println(object);
        }
    }

    @Test
    public void test9() {
        Method[] methods = MethodUtil.getPublicMethods(User.class);
        Arrays.stream(methods).forEach(method -> System.out.println(method.getName()));
    }
}

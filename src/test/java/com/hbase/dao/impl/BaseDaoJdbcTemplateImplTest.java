package com.hbase.dao.impl;

import com.hbase.test.entity.User;
import com.hbase.dao.BaseDao;
import com.hbase.entity.jdbc.Compare;
import com.hbase.entity.jdbc.Condition;
import com.hbase.entity.jdbc.QueryCondition;
import com.hbase.entity.jdbc.UpdateParams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring.xml"})
public class BaseDaoJdbcTemplateImplTest {

    private static final String USER = "user";

    @Inject
    private BaseDao baseDao;

    @Test
    public void findById() throws Exception {
        System.out.println(baseDao.findById(USER, 1, "id xx,name,phone,sex"));
    }

    @Test
    public void findById2() throws Exception {
        System.out.println(baseDao.findById(USER, 1, "id,name,phone,sex", User.class));
    }

    @Test
    public void findCount() throws Exception {
        System.out.println(baseDao.findCount(USER, Condition.create().addCondition("id", 1)));
    }

    @Test
    public void find() throws Exception {
        System.out.println(baseDao.find(USER, "id,name,phone", QueryCondition.create(1, 20)
                .setOrder("phone desc")));
    }

    @Test
    public void find2() throws Exception {
        System.out.println(baseDao.find(USER, "id,name,phone", QueryCondition.create(1, 20)
                .setOrder("phone desc"),User.class));
    }

    @Test
    public void add() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "lishi");
        map.put("age", 20);
        System.out.println(baseDao.add("user", map));
    }

    @Test
    public void add2() throws Exception {
        User user = new User();
        user.setName("wangwu");
        user.setAge(33);
        user.setPhone("18688888888");
        user.setSex(1);
        System.out.println(baseDao.addObj("user", user));
    }

    @Test
    public void delete() throws Exception {
        System.out.println(baseDao.delete(USER, Condition.create().addCondition("id", 57717, Compare.GREATER_OR_EQUAL)));
    }

    @Test
    public void deleteById() throws Exception {
        System.out.println(baseDao.deleteById(USER, 57732));
    }

    @Test
    public void update() throws Exception {
        System.out.println(baseDao.update(USER, UpdateParams.create()
                .addData("name", "zhangsan")
                .addCondition("name", "wangwu")));
    }

    @Test
    public void updateById() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "lishi66");
        System.out.println(baseDao.updateById(USER, map, 16));
    }

}
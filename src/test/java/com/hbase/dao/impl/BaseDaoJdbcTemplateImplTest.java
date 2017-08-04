package com.hbase.dao.impl;

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
@ContextConfiguration(locations = { "classpath:spring.xml" })
public class BaseDaoJdbcTemplateImplTest {

    public static final String X_MEMBER = "x_member";
    @Inject
    private BaseDao baseDao;

    @Test
    public void findById() throws Exception {
        System.out.println(baseDao.findById(X_MEMBER,606,"id,name,phone,token"));
    }

    @Test
    public void findCount() throws Exception {
        System.out.println(baseDao.findCount(X_MEMBER,Condition.create().addCondition("id",606)));
    }

    @Test
    public void find() throws Exception {
        System.out.println(baseDao.find(X_MEMBER,"id,name,phone,token", QueryCondition.create(1,20)
        .setOrder("phone desc")));
    }

    @Test
    public void add() throws Exception {
        Map<String,Object> map = new HashMap<>();
        map.put("name","zhangsan");
        map.put("age",20);
        System.out.println(baseDao.add("test", map));
    }

    @Test
    public void delete() throws Exception {
        System.out.println(baseDao.delete(X_MEMBER,Condition.create().addCondition("id",57717, Compare.GREATER_OR_EQUAL)));
    }

    @Test
    public void deleteById() throws Exception {
        System.out.println(baseDao.deleteById(X_MEMBER,57732));
    }

    @Test
    public void update() throws Exception {
        System.out.println(baseDao.update(X_MEMBER, UpdateParams.create()
                .addData("name","wangwu")
                .addCondition("name","zhangsan")));
    }

    @Test
    public void updateById() throws Exception {
        Map<String,Object> map = new HashMap<>();
        map.put("name","lishi66");
        System.out.println(baseDao.updateById(X_MEMBER,map,57739));
    }

}
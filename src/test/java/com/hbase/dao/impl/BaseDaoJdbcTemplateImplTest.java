package com.hbase.dao.impl;

import com.hbase.dao.BaseDao;
import com.hbase.entity.jdbc.ColumnOps;
import com.hbase.entity.jdbc.Condition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring.xml" })
public class BaseDaoJdbcTemplateImplTest {

    public static final String X_MEMBER = "x_member";
    @Inject
    private BaseDao baseDao;

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseDaoJdbcTemplateImpl.class);

    @Test
    public void findById() throws Exception {
        System.out.println(baseDao.findById(X_MEMBER,606,"id,name,phone,token"));
    }

    @Test
    public void findCount() throws Exception {
        Map<ColumnOps,Object> condition = new LinkedHashMap<>();
        condition.put(new ColumnOps("id", ColumnOps.CompareOp.EQUAL),606);
        System.out.println(baseDao.findCount(X_MEMBER,condition));
    }

    @Test
    public void findCount1() throws Exception {
        System.out.println(baseDao.findCount(X_MEMBER,null,Condition.create().setIn("id IN(6,7,8,606,587)")));
    }

    @Test
    public void find() throws Exception {
    }

    @Test
    public void findByEq() throws Exception {
    }

    @Test
    public void add() throws Exception {
        Map<String,Object> map = new HashMap<>();
        map.put("name","zhangsan");
        System.out.println(baseDao.add(X_MEMBER, map));
    }

    @Test
    public void delete() throws Exception {
        Map<String,Object> map = new HashMap<>();
        map.put("name","zhangsan");
        System.out.println(baseDao.delete(X_MEMBER,map));
    }

    @Test
    public void deleteById() throws Exception {
        System.out.println(baseDao.deleteById(X_MEMBER,57732));
    }

    @Test
    public void update() throws Exception {
        Map<String,Object> map = new HashMap<>();
        map.put("name","lishi");
        Map<String,Object> condition = new HashMap<>();
        condition.put("name","zhangsan");
        System.out.println(baseDao.update(X_MEMBER,map,condition));
    }

    @Test
    public void updateById() throws Exception {
        Map<String,Object> map = new HashMap<>();
        map.put("name","lishi66");
        System.out.println(baseDao.updateById(X_MEMBER,map,57737));
    }

}
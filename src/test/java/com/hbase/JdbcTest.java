package com.hbase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Map;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring.xml" })
public class JdbcTest {

    @Autowired
    private JdbcTemplate template;

    @Test
    public void test1(){
        List<Map<String, Object>> map = template.queryForList("SELECT * FROM x_car_run_log");
        System.out.println(map.size());
    }

}

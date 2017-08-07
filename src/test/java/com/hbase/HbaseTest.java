package com.hbase;

import com.hbase.dao.BaseDao;
import com.hbase.entity.jdbc.QueryCondition;
import com.hbase.utils.Lpn;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HbaseTest {

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(50);

    private static BaseDao baseDao = new ClassPathXmlApplicationContext("spring.xml").getBean(BaseDao.class);

    public static void main(String[] args) {
//        createTalble();
        deleteTable();
    }

    public static void createTalble() {
        int i = 0;
        final int SIZE = 50;
        while (true) {
            List<Map<String, Object>> list = baseDao.find("x_car_run", "lpn", QueryCondition.create(i++, SIZE));
            if (list.isEmpty()) break;
            list.forEach(map -> {
                String lpn = MapUtils.getString(map, "lpn");
                if (StringUtils.isNotBlank(lpn)) {
                    EXECUTOR.execute(() -> HbaseUtils.createTable(Lpn.encode(lpn), "info"));
                }
            });
            System.out.println(list.size());
            if (list.size() < SIZE) break;
        }
        EXECUTOR.shutdown();
    }

    public static void deleteTable() {
        try {
            List<String> tables = HbaseUtils.listTables();
            System.out.println(tables);
            tables.forEach(table -> EXECUTOR.execute(() -> HbaseUtils.deleteTable(table)));
            EXECUTOR.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

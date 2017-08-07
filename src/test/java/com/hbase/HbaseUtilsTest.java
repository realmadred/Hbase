package com.hbase;

import com.hbase.dao.BaseDao;
import com.hbase.entity.HBaseResult;
import com.hbase.entity.HbaseConditionEntity;
import com.hbase.entity.HbaseDataOneFamily;
import com.hbase.entity.jdbc.Compare;
import com.hbase.entity.jdbc.QueryCondition;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:spring.xml"})
public class HbaseUtilsTest {

    @Inject
    private BaseDao baseDao;

    @Test
    public void insertList() throws Exception {
        StopWatch stopWatch = new StopWatch();
        for (int i = 0; i < 10; i++) {
            stopWatch.start("list");
            List<Map<String, Object>> maps = baseDao.find("x_car_run_log_20170602", "id,lpn,pile_id,member_id,latitude,longitude,speed,bat_status,door_status,car_signal,total_kg,total_minute,endurance_mileage,rest_battery,order_id,create_time,address,mileage,small_battery_voltage,small_battery_charge_status,gear,handBreakStatus,lightStatus,safeBeltStatus,windowStatus,readyStatus,trunkStatus,cpuTemperature,netCount,networkFlow,isWifiOn,isGpsOn,weightNum,isCarOnline",
                    QueryCondition.create(i, 100).setOrder("create_time desc"));
            List<HbaseDataOneFamily> data = new ArrayList<>(100);
            maps.forEach(map -> {
                HbaseDataOneFamily family = new HbaseDataOneFamily();
                family.setKey(Objects.toString(map.get("lpn")) + Objects.toString(map.get("create_time")));
                family.setColumnMap(map);
                data.add(family);
            });
            // 使用单线程
            HbaseUtils.insertList("car_run", "info", data);
            data.clear();
            stopWatch.stop();
            System.out.println(" 运行时间： " + stopWatch);
        }
    }

    @Test
    public void insert() throws IOException {
        final String table = "emp1";
        final String cf = "cf1";
        List<HbaseDataOneFamily> families = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            HbaseDataOneFamily family = new HbaseDataOneFamily().setKey("row" + i)
                    .putData("name", i).putData("age", i);
            families.add(family);
        }
        HbaseUtils.insertList(table, cf, families);
    }

    @Test
    public void delete() throws IOException {
        final String table = "emp1";
        final String cf = "cf1";
        for (int i = 0; i < 15; i++) {
            HbaseUtils.delete(table, "row" + i, cf, "name");
        }
    }

    @Test
    public void scanTable() throws IOException {
        final String table = "emp1";
        List<HBaseResult> results = HbaseUtils.scanTable(table, "row1", "row11", 10);
        System.out.println("--------------------------------------");
        System.out.println(results.size());
        System.out.println("--------------------------------------");
        System.out.println(results);
    }

    @Test
    public void scanTableByCondition() throws IOException {
        final String table = "emp1";
        List<HbaseConditionEntity> hbaseConditions = new ArrayList<>();
        HbaseConditionEntity entity = new HbaseConditionEntity("cf1", "age", 15,
                FilterList.Operator.MUST_PASS_ALL, CompareFilter.CompareOp.LESS);
        hbaseConditions.add(entity);
        List<HBaseResult> results = HbaseUtils.scanByConditions(table, "row1", "row11", 10, hbaseConditions);
        System.out.println("--------------------------------------");
        System.out.println(results.size());
        System.out.println("--------------------------------------");
        System.out.println(results);
    }

    @Test
    public void createTalble() {
        int i = 0;
        final int SIZE = 50;
        while (true) {
            List<Map<String, Object>> list = baseDao.find("x_car_run", "lpn", QueryCondition.create(i++, SIZE)
                    .addCondition("id",210, Compare.GREATER));
            if (list.isEmpty()) break;
            list.forEach(map -> {
                String lpn = MapUtils.getString(map, "lpn");
                if (StringUtils.isNotBlank(lpn)) {
                    HbaseUtils.createTable(lpn.substring(1), "info");
                }
            });
            System.out.println(list.size());
            if (list.size() < SIZE) break;
        }
    }

    @Test
    public void deleteTable() throws IOException {
        List<String> tables = HbaseUtils.listTables();
        System.out.println(tables);
        tables.forEach(HbaseUtils::deleteTable);
    }

}
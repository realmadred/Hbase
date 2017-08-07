package com.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.hbase.dao.BaseDao;
import com.hbase.entity.HbaseDataOneFamily;
import com.hbase.entity.jdbc.QueryCondition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

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
                family.setColumnMap(Maps.transformValues(map, new Function<Object, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable Object input) {
                        return Objects.toString(input);
                    }
                }));
                data.add(family);
            });
            // 使用单线程
            HbaseUtils.insertList("car_run","info",data);
            data.clear();
            stopWatch.stop();
            System.out.println(" 运行时间： "+stopWatch);
        }
    }

}
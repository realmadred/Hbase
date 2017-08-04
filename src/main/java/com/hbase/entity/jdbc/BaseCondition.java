package com.hbase.entity.jdbc;

import java.util.Map;

public interface BaseCondition {

    Map<String, String> getInMap();

    Map<String, String> getBetweenMap();

    Map<ColumnOps, Object> getConditionMap();

}

package com.hbase.dao;

import com.hbase.entity.jdbc.ColumnOps;
import com.hbase.entity.jdbc.Condition;
import com.hbase.entity.jdbc.QueryCondition;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * 数据库访问基础类
 * lf
 * 2017-08-03 11:16:47
 */
public interface BaseDao {

    /**
     * 根据id查询
     * 2017-08-03 11:16:42
     *
     * @param table  表
     * @param id     id
     * @param fields 字段
     * @return
     */
    Map<String, Object> findById(String table, Object id, String fields);

     /**
     * 查询数量
     *
     * @param table        表
     * @param conditionMap 条件
     * @return 数量
     */
    int findCount(String table, Map<ColumnOps, Object> conditionMap);

    /**
     * 查询数量
     *
     * @param table        表
     * @param conditionMap 条件
     * @param condition    in between
     * @return 数量
     */
    int findCount(String table, Map<ColumnOps, Object> conditionMap, Condition condition);

    /**
     * 根据条件查询
     *
     * @param table           表
     * @param conditionMap 字段条件
     * @param fields          查询字段
     * @param condition       order by , having, group by , limit 条件
     * @return
     */
    List<Map<String, Object>> find(String table, String fields, Map<ColumnOps, Object> conditionMap, QueryCondition condition);

    /**
     * 根据条件查询 都是等于
     *
     * @param table           表
     * @param columnCondition 字段条件
     * @param fields          查询字段
     * @param condition       order by , having, group by , limit 条件
     * @return
     */
    List<Map<String, Object>> findByEq(String table, Map<String, Object> conditionMap, String fields, QueryCondition condition);

    /**
     * 插入单条数据
     *
     * @param table b
     * @param data  数据
     * @return 自增id
     */
    int add(String table, Map<String, Object> data);

    /**
     * 删除
     *
     * @param table        表名
     * @param conditionMap 条件 必须要有
     * @return 删除的条数
     */
    int delete(String table, @Nonnull Map<String, Object> conditionMap);

    /**
     * 根据id删除
     *
     * @param table 表名
     * @param id    id
     * @return 删除的条数
     */
    int deleteById(String table, Object id);

    /**
     * 更新数据
     *
     * @param table     表
     * @param data      需要更新的数据
     * @param condition 更新条件
     * @return 更新的条数
     */
    int update(String table, Map<String, Object> data, Map<String, Object> condition);

    /**
     * 更新数据
     *
     * @param table 表
     * @param data  需要更新的数据
     * @param id    更新条件
     * @return 更新的条数
     */
    int updateById(String table, Map<String, Object> data, Object id);
}
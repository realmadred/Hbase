package com.hbase.dao.impl;

import com.hbase.dao.BaseDao;
import com.hbase.entity.jdbc.ColumnOps;
import com.hbase.entity.jdbc.Condition;
import com.hbase.entity.jdbc.QueryCondition;
import com.hbase.utils.Common;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.annotation.Nonnull;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

import static com.hbase.dao.impl.BaseDaoJdbcTemplateImpl.KeyWord.*;

/**
 * 使用springJdbcTemplate实现的baseDao
 * lf
 * 2017-08-03 11:33:35
 */
@Repository
public class BaseDaoJdbcTemplateImpl implements BaseDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseDaoJdbcTemplateImpl.class);

    @Autowired
    private JdbcTemplate template;

    private static final Map<String, Object> EMPTY_MAP = new HashMap<>(0);
    private static final List<Map<String, Object>> EMPTY_LIST = new ArrayList<>(0);

    /**
     * 根据id查询
     * 2017-08-03 11:16:42
     *
     * @param table  表
     * @param id     id
     * @param fields 字段
     * @return
     */
    @Override
    public Map<String, Object> findById(String table, Object id, String fields) {
        if (Common.hasEmpty(table, id, fields)) return EMPTY_MAP;
        StringBuilder sql = new StringBuilder()
                .append(SELECT)
                .append(fields).append(FROM)
                .append(table).append(WHERE_ID);
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,id);
        return template.queryForMap(sql.toString(), id);
    }

    /**
     * 查询数量
     *
     * @param table        表
     * @param conditionMap 条件
     * @return 数量
     */
    @Override
    public int findCount(String table, Map<ColumnOps, Object> conditionMap) {
        // 判断参数
        if (StringUtils.isBlank(table)) return 0;
        StringBuilder sql = new StringBuilder();
        sql.append(SELECT_COUNT_FROM).append(table);
        // 判断条件
        if (!CollectionUtils.isEmpty(conditionMap)) {
            sql.append(WHERE);
            Object[] args = new Object[conditionMap.size()];
            handleArgs(conditionMap, sql, args);
            LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
            return template.queryForObject(sql.toString(), Integer.class, args);
        }
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql);
        return template.queryForObject(sql.toString(), Integer.class);
    }

    /**
     * 查询数量
     *
     * @param table        表
     * @param conditionMap 条件
     * @param condition    in between
     * @return 数量
     */
    @Override
    public int findCount(String table, Map<ColumnOps, Object> conditionMap, Condition condition) {
        // 判断参数
        if (StringUtils.isBlank(table)) return 0;
        StringBuilder sql = new StringBuilder();
        sql.append(SELECT_COUNT_FROM).append(table)
                .append(WHERE);
        // 判断条件
        if (condition != null) {
            sql.append(condition);
        }
        if (CollectionUtils.isEmpty(conditionMap)) return template.queryForObject(sql.toString(), Integer.class);
        Object[] args = new Object[conditionMap.size()];
        // 处理参数
        handleArgs(conditionMap, sql, args);
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        return template.queryForObject(sql.toString(), Integer.class, args);
    }

    /**
     * 参数处理
     *
     * @param conditionMap 条件
     * @param sql          sql部分
     * @return
     */
    private void handleArgs(Map<ColumnOps, Object> conditionMap, StringBuilder sql, Object[] args) {
        int i = 0;
        for (Map.Entry<ColumnOps, Object> entry : conditionMap.entrySet()) {
            sql.append(AND).append(entry.getKey());
            args[i++] = entry.getValue();
        }
    }

    /**
     * 根据条件查询
     *
     * @param table        表
     * @param conditionMap 字段条件
     * @param fields       查询字段
     * @param condition    order by , having, group by , limit 条件
     * @return
     */
    @Override
    public List<Map<String, Object>> find(String table, String fields, Map<ColumnOps, Object> conditionMap, QueryCondition condition) {
        if (Common.hasEmpty(table, fields)) return EMPTY_LIST;
        StringBuilder sql = new StringBuilder(32);
        sql.append(SELECT).append(fields).append(table)
                .append(WHERE);
        if (condition != null) {
            sql.append(condition);
        }
        if (CollectionUtils.isEmpty(conditionMap)) return template.queryForList(sql.toString());
        Object[] args = new Object[conditionMap.size()];
        handleArgs(conditionMap, sql, args);
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        return template.queryForList(sql.toString(), args);
    }

    /**
     * 根据条件查询 都是等于
     *
     * @param table        表
     * @param conditionMap 字段条件
     * @param fields       查询字段
     * @param condition    order by , having, group by , limit 条件
     * @return
     */
    @Override
    public List<Map<String, Object>> findByEq(String table, Map<String, Object> conditionMap, String fields, QueryCondition condition) {
        if (Common.hasEmpty(table, fields)) return EMPTY_LIST;
        StringBuilder sql = new StringBuilder(32);
        sql.append(SELECT).append(fields).append(table)
                .append(WHERE);
        if (condition != null) {
            sql.append(condition);
        }
        if (CollectionUtils.isEmpty(conditionMap)) return template.queryForList(sql.toString());
        Object[] args = new Object[conditionMap.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : conditionMap.entrySet()) {
            sql.append(AND).append(entry.getKey()).append(EQUAL_PARAM);
            args[i++] = entry.getValue();
        }
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        return template.queryForList(sql.toString(), args);
    }

    /**
     * 插入单条数据
     *
     * @param table b
     * @param data  数据
     * @return 自增id
     */
    @Override
    public int add(String table, Map<String, Object> data) {
        if (StringUtils.isBlank(table) || CollectionUtils.isEmpty(data)) return 0;
        final StringBuilder sql = new StringBuilder()
                .append(INSERT)
                .append(table).append("(");
        int size = data.size();
        StringBuilder values = new StringBuilder(size << 1 + 1).append(VALUES);
        int i = 0;
        final Object[] args = new Object[size];
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            args[i] = entry.getValue();
            if (++i >= size) {
                sql.append(entry.getKey()).append(")");
                values.append("?)");
            } else {
                sql.append(entry.getKey()).append(",");
                values.append("?,");
            }
        }
        sql.append(values);
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        template.update((con) -> {
            PreparedStatement ps = con.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            for (int j = 0; j < args.length; j++) {
                ps.setObject(j + 1, args[j]);
            }
            return ps;
        }, keyHolder);
        return keyHolder.getKey().intValue();
    }

    /**
     * 删除
     *
     * @param table        表名
     * @param conditionMap 条件 必须要有
     * @return 删除的条数
     */
    @Override
    public int delete(String table, @Nonnull Map<String, Object> conditionMap) {
        // 必须要又条件
        if (StringUtils.isBlank(table) || CollectionUtils.isEmpty(conditionMap)) return 0;
        StringBuilder sql = new StringBuilder()
                .append(DELETE)
                .append(table).append(WHERE);
        Object[] args = new Object[conditionMap.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : conditionMap.entrySet()) {
            sql.append(AND).append(entry.getKey()).append(EQUAL_PARAM);
            args[i++] = entry.getValue();
        }
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        return template.update(sql.toString(), args);
    }

    /**
     * 根据id删除
     *
     * @param table 表名
     * @param id    id
     * @return 删除的条数
     */
    @Override
    public int deleteById(String table, Object id) {
        // 必须要又条件
        if (Common.hasEmpty(table, id)) return 0;
        StringBuilder sql = new StringBuilder()
                .append(DELETE)
                .append(table).append(WHERE_ID);
        LOGGER.info(sql.toString());
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,id);
        return template.update(sql.toString(), id);
    }

    /**
     * 更新数据
     *
     * @param table     表
     * @param data      需要更新的数据
     * @param condition 更新条件
     * @return 更新的条数
     */
    @Override
    public int update(String table, Map<String, Object> data, Map<String, Object> condition) {
        // 更新必须要有条件
        if (StringUtils.isBlank(table) ||
                CollectionUtils.isEmpty(data) ||
                CollectionUtils.isEmpty(condition)) return 0;
        final StringBuilder sql = new StringBuilder()
                .append(UPDATE)
                .append(table).append(SET);
        int size1 = data.size();
        int size = size1 + condition.size();
        int i = 0;
        final Object[] args = new Object[size];
        // 需要修改的字段
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            args[i] = entry.getValue();
            if (++i >= size1) {
                sql.append(entry.getKey()).append(EQUAL_PARAM);
            } else {
                sql.append(entry.getKey()).append(EQUAL_PARAM).append(",");
            }
        }
        // 条件
        sql.append(WHERE);
        for (Map.Entry<String, Object> entry : condition.entrySet()) {
            args[i++] = entry.getValue();
            sql.append(AND).append(entry.getKey()).append(EQUAL_PARAM);
        }
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        return template.update(sql.toString(), args);
    }

    /**
     * 更新数据
     *
     * @param table 表
     * @param data  需要更新的数据
     * @param id    更新条件
     * @return 更新的条数
     */
    @Override
    public int updateById(String table, Map<String, Object> data, Object id) {
        // 更新必须要有条件
        if (StringUtils.isBlank(table) || Common.isEmpty(id) ||
                CollectionUtils.isEmpty(data)) return 0;
        final StringBuilder sql = new StringBuilder()
                .append(UPDATE)
                .append(table).append(SET);
        int size = data.size();
        int i = 0;
        final Object[] args = new Object[size + 1];
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            args[i++] = entry.getValue();
            if (i >= size) {
                sql.append(entry.getKey()).append(EQUAL_PARAM);
            } else {
                sql.append(entry.getKey()).append(EQUAL_PARAM).append(",");
            }
        }
        sql.append(WHERE_ID);
        args[i] = id;
        LOGGER.info(">>>>>>>>sql:{}\n args:{}",sql,Arrays.toString(args));
        return template.update(sql.toString(), args);
    }

    enum KeyWord {
        SELECT("SELECT "),
        FROM(" FROM "),
        UPDATE("UPDATE "),
        DELETE("DELETE FROM "),
        INSERT("INSERT INTO "),
        VALUES("VALUES("),
        WHERE(" WHERE 1 = 1 "),
        WHERE_ID(" WHERE id = ? "),
        AND(" AND "),
        SET(" SET "),
        EQUAL_PARAM(" = ? "),
        SELECT_COUNT_FROM("SELECT COUNT(*) FROM ");

        String value;

        KeyWord(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}

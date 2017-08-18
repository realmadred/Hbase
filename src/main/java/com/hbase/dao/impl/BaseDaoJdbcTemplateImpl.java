package com.hbase.dao.impl;

import com.hbase.dao.BaseDao;
import com.hbase.entity.jdbc.*;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hbase.dao.impl.BaseDaoJdbcTemplateImpl.KeyWord.*;

/**
 * 使用springJdbcTemplate实现的baseDao
 * lf
 * 2017-08-03 11:33:35
 */
@Repository
public class BaseDaoJdbcTemplateImpl implements BaseDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseDaoJdbcTemplateImpl.class);
    private static final String TO_TRIM = "1 = 1  AND "; // 这个sql片段trim掉

    @Autowired
    private JdbcTemplate template;

    private static final Map<String, Object> EMPTY_MAP = Common.EMPTY_MAP;
    private static final List<Map<String, Object>> EMPTY_LIST = new ArrayList<>(0);
    private static final String SEPARATOR = ",";
    private static final String PARAM = "?";

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
        LOGGER.info(">>>>>>>>sql:{}\n args:{}", sql, id);
        return template.queryForMap(sql.toString(), id);
    }

    /**
     * 根据id查询
     * 返回制定的类型
     * 2017-08-17 09:34:44
     *
     * @param table  表
     * @param id     id
     * @param fields 字段
     * @param clazz
     * @return
     */
    @Override
    public <T> T findById(String table, Object id, String fields, Class<T> clazz) {
        Map<String, Object> map = findById(table, id, fields);
        return Common.toObject(map,clazz);
    }

    /**
     * 查询数量
     *
     * @param table     表
     * @param condition 查询条件
     * @return 数量
     */
    @Override
    public int findCount(String table, Condition condition) {
        // 判断参数
        if (StringUtils.isBlank(table)) return 0;
        StringBuilder sql = new StringBuilder();
        sql.append(SELECT_COUNT_FROM).append(table);
        // 判断条件
        if (condition == null) {
            return template.queryForObject(sql.toString(), Integer.class);
        }
        // where条件
        sql.append(WHERE);
        // 参数
        List<Object> args = new ArrayList<>();
        handleCondition(condition, sql, args);
        LOGGER.info(">>>>>>>>args:{}", sql, args);
        if (args.isEmpty()) return template.queryForObject(checkSql(sql), Integer.class);
        return template.queryForObject(checkSql(sql), Integer.class, args.toArray());
    }

    /**
     * 根据条件查询
     *
     * @param table     表
     * @param fields    查询字段
     * @param condition 条件
     * @return
     */
    @Override
    public List<Map<String, Object>> find(String table, String fields, @Nonnull QueryCondition condition) {
        if (Common.hasEmpty(table, fields, condition)) return EMPTY_LIST;
        StringBuilder sql = new StringBuilder(32);
        sql.append(SELECT).append(fields).append(FROM).append(table).append(WHERE);

        // 处理条件
        List<Object> args = new ArrayList<>();
        handleCondition(condition, sql, args);
        // 追加其它的order group limit等等
        sql.append(condition);
        LOGGER.info(">>>>>>>>args:{}", args);
        return template.queryForList(checkSql(sql), args.toArray());
    }

    /**
     * 根据条件查询
     *
     * @param table     表
     * @param fields    查询字段
     * @param condition order by , having, group by , limit 条件
     * @param clazz
     * @return
     */
    @Override
    public <T> List<T> find(String table, String fields, QueryCondition condition,final Class<T> clazz) {
        final List<Map<String, Object>> maps = find(table, fields, condition);
        final List<T> list = new ArrayList<>(maps.size());
        maps.parallelStream().forEach(map -> list.add(Common.toObject(map,clazz)));
        return list;
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
        LOGGER.info(">>>>>>>>sql:{}\n args:{}", sql, Arrays.toString(args));
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        template.update((con) -> {
            PreparedStatement ps = con.prepareStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
            for (int j = 0; j < args.length; j++) {
                ps.setObject(j + 1, args[j]);
            }
            return ps;
        }, keyHolder);
        final Number key = keyHolder.getKey();
        return key == null ? 1 : key.intValue();
    }

    /**
     * 插入单条数据
     *
     * @param table b
     * @param obj   数据
     * @return 自增id
     */
    @Override
    public int addObj(String table, Object obj) {
        return add(table,Common.toMap(obj));
    }

    /**
     * 删除
     *
     * @param table     表名
     * @param condition 条件 必须要有
     * @return 删除的条数
     */
    @Override
    public int delete(String table, @Nonnull Condition condition) {
        // 必须要又条件
        if (StringUtils.isBlank(table)) return 0;
        StringBuilder sql = new StringBuilder()
                .append(DELETE)
                .append(table).append(WHERE);
        List<Object> args = new ArrayList<>();
        handleCondition(condition, sql, args);
        LOGGER.info(">>>>>>>>args:{}", args);
        return template.update(checkSql(sql), args.toArray());
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
        LOGGER.info(checkSql(sql));
        LOGGER.info(">>>>>>>>sql:{}\n id:{}", sql, id);
        return template.update(sql.toString(), id);
    }

    /**
     * 更新数据
     *
     * @param table  表
     * @param params 参数包括数据和条件
     * @return 更新的条数
     */
    @Override
    public int update(String table, UpdateParams params) {
        // 更新必须要有条件
        if (StringUtils.isBlank(table) || params == null) return 0;
        final StringBuilder sql = new StringBuilder()
                .append(UPDATE)
                .append(table).append(SET);

        Map<String, Object> data = params.getData();
        if (CollectionUtils.isEmpty(data)) return 0;

        List<Object> args = new ArrayList<>();
        // 需要修改的字段
        int size = data.size();
        int i = 0;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            args.add(entry.getValue());
            if (++i >= size) {
                sql.append(entry.getKey()).append(EQUAL_PARAM);
            } else {
                sql.append(entry.getKey()).append(EQUAL_PARAM).append(SEPARATOR);
            }
        }
        // 条件
        sql.append(WHERE);
        handleCondition(params, sql, args);
        LOGGER.info(">>>>>>>>args:{}", args);
        return template.update(checkSql(sql), args.toArray());
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
        LOGGER.info(">>>>>>>>sql:{}\n args:{}", sql, Arrays.toString(args));
        return template.update(sql.toString(), args);
    }

    /**
     * 参数处理
     *
     * @param conditionMap 条件
     * @param sql          sql部分
     * @return
     */
    private void handleArgs(Map<ColumnOps, Object> conditionMap, StringBuilder sql, List<Object> args) {
        conditionMap.forEach((key, value) -> {
            sql.append(AND).append(key);
            args.add(value);
        });
    }

    /**
     * 处理条件
     *
     * @param condition 条件
     * @param sql       sql
     * @return
     */
    private void handleCondition(@Nonnull BaseCondition condition, StringBuilder sql, List<Object> args) {
        Map<ColumnOps, Object> conditionMap = condition.getConditionMap();
        Map<String, String> inMap = condition.getInMap();
        Map<String, String> betweenMap = condition.getBetweenMap();

        if (!CollectionUtils.isEmpty(conditionMap)) {
            // 处理参数
            handleArgs(conditionMap, sql, args);
        }

        // in
        if (!CollectionUtils.isEmpty(inMap)) {
            inMap.forEach((key, value) -> {
                if (!Common.hasEmpty(key, value)) {
                    sql.append(AND).append(key).append(IN);
                    String[] splits = value.split(SEPARATOR);
                    for (int i = 0, len = splits.length; i < len; i++) {
                        if (i >= len - 1) {
                            sql.append(PARAM).append(")");
                            args.add(splits[i]);
                        } else {
                            sql.append(PARAM).append(SEPARATOR);
                            args.add(splits[i]);
                        }
                    }
                }
            });
        }

        // between
        if (!CollectionUtils.isEmpty(betweenMap)) {
            betweenMap.forEach((key, value) -> {
                if (!Common.hasEmpty(key, value) && value.contains(SEPARATOR)) {
                    sql.append(AND).append(key).append(BETWEEN);
                    String[] splits = value.split(SEPARATOR);
                    sql.append(PARAM).append(SEPARATOR);
                    args.add(splits[0]);
                    args.add(splits[1]);
                }
            });
        }
    }

    /**
     * 统一过滤sql
     *
     * @param sql
     * @return
     */
    private String checkSql(@Nonnull StringBuilder sql) {
        String replace = sql.toString().replace(TO_TRIM, "");
        LOGGER.info("--------------sql:{}", replace);
        return replace;
    }

    enum KeyWord {
        SELECT("SELECT "),
        FROM(" FROM "),
        UPDATE("UPDATE "),
        DELETE("DELETE FROM "),
        INSERT("INSERT INTO "),
        VALUES(" VALUES("),
        IN(" IN("),
        BETWEEN(" BETWEEN ? AND ? "),
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

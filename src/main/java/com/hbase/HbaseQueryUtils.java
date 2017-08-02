package com.hbase;

import com.hbase.entity.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * HBase 数据库访问工具类
 * lf
 * 2017-08-02 16:13:49
 */
public class HbaseQueryUtils {

    private static final Log LOG = LogFactory.getLog(HbaseQueryUtils.class);
    private static final Configuration CONFIGURATION;
    private static Connection connection;
    private static Admin admin;
    private static AggregationClient aggregationClient;
    private static final int DEFAULT_SIZE;

    static {
        DEFAULT_SIZE = 10;
        CONFIGURATION = HBaseConfiguration.create();
        CONFIGURATION.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        CONFIGURATION.set(HConstants.ZOOKEEPER_QUORUM, "192.168.127.129,192.168.127.132,192.168.127.133");
        CONFIGURATION.set(HConstants.MASTER_PORT, "16000");
        CONFIGURATION.set(HConstants.HBASE_DIR, "hdfs://192.168.127.129:9000/hbase");
        CONFIGURATION.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "5000");
        try {
            connection = ConnectionFactory.createConnection(CONFIGURATION);
            admin = connection.getAdmin();
            aggregationClient = new AggregationClient(CONFIGURATION);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> close()));
        } catch (IOException e) {
            LOG.error("hbase error!", e);
        }
    }

    /**
     * 关闭连接
     */
    private static void close() {
        try {
            LOG.info("close！");
            if (null != admin) {
                admin.close();
            }
        } catch (IOException e) {
            LOG.error("hbase close error!", e);
        } finally {
            if (null != connection) {
                try {
                    connection.close();
                } catch (IOException e) {
                    LOG.error("hbase connection close error!", e);
                } finally {
                    if (aggregationClient != null) {
                        try {
                            aggregationClient.close();
                        } catch (IOException e) {
                            LOG.error("hbase aggregationClient close error!", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 创建表
     *
     * @param tableName  表名
     * @param colFamilys 列族列表
     * @throws IOException
     */
    public static void createTable(String tableName, String[] colFamilys) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            log(tableName + " exists.");
        } else {
            HTableDescriptor hTableDesc = new HTableDescriptor(tName);
            for (String col : colFamilys) {
                HColumnDescriptor hColumnDesc = new HColumnDescriptor(col);
                hColumnDesc.setBlockCacheEnabled(true);
                hColumnDesc.setInMemory(true);
                hTableDesc.addFamily(hColumnDesc);
            }
            admin.createTable(hTableDesc);
        }
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param col       列族
     * @throws IOException
     */
    public static void createTable(String tableName, String col) throws IOException {
        createTable(tableName, new String[]{col});
    }

    /**
     * 创建Namespace
     *
     * @param namespace namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(descriptor);
    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
        } else {
            log(tableName + " not exists.");
        }
    }

    /**
     * 表描述
     *
     * @param tableName 表名称
     * @throws IOException
     */
    public static void descTable(String tableName) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            HTableDescriptor descriptor = new HTableDescriptor(tName);
            log(descriptor.getColumnFamilies());
        } else {
            log(tableName + " not exists.");
        }
    }

    /**
     * 添加列族
     *
     * @param tableName 表名称
     * @param cf        列族
     * @throws IOException
     */
    public static void addColumn(String tableName, String cf) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            admin.addColumn(tName, columnDescriptor);
        } else {
            log(tableName + " not exists.");
        }
    }

    /**
     * 添加列族
     *
     * @param tableName 表名称
     * @param cfs       列族
     * @throws IOException
     */
    public static void addColumns(String tableName, String[] cfs) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            HColumnDescriptor columnDescriptor;
            for (int i = 0; i < cfs.length; i++) {
                columnDescriptor = new HColumnDescriptor(cfs[i]);
                admin.addColumn(tName, columnDescriptor);
            }
        } else {
            log(tableName + " not exists.");
        }
    }

    /**
     * 删除列族
     *
     * @param tableName 表名称
     * @throws IOException
     */
    public static void delColumn(String tableName, String cf) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
            admin.deleteColumn(tName, columnDescriptor.getName());
        } else {
            log(tableName + " not exists.");
        }
    }

    /**
     * 删除列族
     *
     * @param tableName 表名称
     * @param cfs       列族
     * @throws IOException
     */
    public static void delColumns(String tableName, String[] cfs) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        if (admin.tableExists(tName)) {
            HColumnDescriptor columnDescriptor;
            for (int i = 0; i < cfs.length; i++) {
                columnDescriptor = new HColumnDescriptor(cfs[i]);
                admin.deleteColumn(tName, columnDescriptor.getName());
            }
        } else {
            log(tableName + " not exists.");
        }
    }

    /**
     * 查看已有表
     *
     * @throws IOException
     */
    public static void listTables() {
        HTableDescriptor hTableDescriptors[] = null;
        try {
            hTableDescriptors = admin.listTables();
        } catch (IOException e) {
            LOG.error("hbase error!", e);
        }
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            log(hTableDescriptor.getNameAsString());
        }
    }

    /**
     * 插入单行
     *
     * @param tableName 表名称
     * @param rowKey    RowKey
     * @param colFamily 列族
     * @param col       列
     * @param value     值
     * @throws IOException
     */
    public static void insert(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
            table.put(put);
        }
    }

    /**
     * 批量添加数据
     *
     * @param list 数据集合
     */
    public static void insertDataList(String tableNameStr, List<HbaseDataFamilys> list) throws IOException {
        if (CollectionUtils.isEmpty(list)) return;
        List<Put> puts = new ArrayList<>(list.size());
        TableName tableName = TableName.valueOf(tableNameStr);
        try (Table table = connection.getTable(tableName)) {
            Put put;
            for (HbaseDataFamilys entity : list) {
                put = new Put(Bytes.toBytes(entity.getKey()));// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY
                Set<Map.Entry<String, Map<String, String>>> entries = entity.getColumns().entrySet();
                for (Map.Entry<String, Map<String, String>> columnFamilys : entries) {
                    // 获取列数据
                    Map<String, String> map = columnFamilys.getValue();
                    // 列族
                    String cf = columnFamilys.getKey();
                    byte[] family = Bytes.toBytes(cf);
                    for (Map.Entry<String, String> columns : map.entrySet()) {
                        String column = columns.getKey();
                        String value = columns.getValue();
                        put.addColumn(family, Bytes.toBytes(column), Bytes.toBytes(value));
                    }
                }
                puts.add(put);
            }
            table.put(puts);
        }
    }

    /**
     * 批量添加数据
     * 制定列族
     *
     * @param list 数据集合
     */
    public static void insertList(String tableNameStr, String columnFamily, List<HbaseDataOneFamily> list) throws IOException {
        if (CollectionUtils.isEmpty(list) || StringUtils.isBlank(tableNameStr)
                || StringUtils.isBlank(columnFamily)) return;
        List<Put> puts = new ArrayList<>(list.size());
        TableName tableName = TableName.valueOf(tableNameStr);
        try (Table table = connection.getTable(tableName)) {
            Put put;
            byte[] family = Bytes.toBytes(columnFamily);
            for (HbaseDataOneFamily entity : list) {
                put = new Put(Bytes.toBytes(entity.getKey()));
                Map<String, String> columnMap = entity.getColumnMap();
                if (MapUtils.isEmpty(columnMap)) continue;
                for (Map.Entry<String, String> columns : columnMap.entrySet()) {
                    String column = columns.getKey();
                    String value = columns.getValue();
                    put.addColumn(family, Bytes.toBytes(column), Bytes.toBytes(value));
                }
                puts.add(put);
            }
            table.put(puts);
        }
    }

    /**
     * 添加数据
     *
     * @param values 数据集合
     */
    public static void insertMap(String tableNameStr, String columnFamily, Map<String, String> values) throws IOException {
        insertMap(tableNameStr, columnFamily, Long.toString(System.currentTimeMillis()), values);
    }

    /**
     * 添加数据
     *
     * @param values 数据集合
     */
    public static void insertMap(String tableNameStr, String columnFamily, String rowKey, Map<String, String> values) throws IOException {
        if (MapUtils.isEmpty(values) || StringUtils.isBlank(tableNameStr)
                || StringUtils.isBlank(columnFamily)) return;
        TableName tableName = TableName.valueOf(tableNameStr);
        try (Table table = connection.getTable(tableName)) {
            byte[] family = Bytes.toBytes(columnFamily);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, String> entry : values.entrySet()) {
                String column = entry.getKey();
                String value = entry.getValue();
                put.addColumn(family, Bytes.toBytes(column), Bytes.toBytes(value));
            }
            table.put(put);
        }
    }

    /**
     * 删除
     *
     * @param tableName 表
     * @param rowKey    行键
     * @param colFamily 列族
     * @param col       列
     * @throws IOException
     */
    public static void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            log(tableName + " not exists.");
        } else {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));
            if (colFamily != null) {
                del.addFamily(Bytes.toBytes(colFamily));
            }
            if (colFamily != null && col != null) {
                del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            }  
            /* 
             * 批量删除 List<Delete> deleteList = new ArrayList<Delete>(); deleteList.add(delete); table.delete(deleteList); 
             */
            table.delete(del);
            table.close();
        }
    }

    /**
     * 删除
     *
     * @param tableName    表
     * @param columnFamily 列族
     * @param list         列
     * @throws IOException
     */
    public static void deleteList(String tableName, String columnFamily, List<HbaseDeleteEntity> list) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            log(tableName + " not exists.");
        } else {
            if (CollectionUtils.isEmpty(list)) return;
            Table table = connection.getTable(TableName.valueOf(tableName));
            int size = list.size();
            List<Delete> deleteList = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                HbaseDeleteEntity deleteFamily = list.get(i);
                if (deleteFamily == null) continue;
                String key = deleteFamily.getKey();
                Delete del = new Delete(Bytes.toBytes(key));
                Set<String> columns = deleteFamily.getColumns();
                if (columnFamily != null) {
                    del.addFamily(Bytes.toBytes(columnFamily));
                    if (CollectionUtils.isNotEmpty(columns)) {
                        Iterator<String> iterator = columns.iterator();
                        String col;
                        while (iterator.hasNext()) {
                            col = iterator.next();
                            del.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(col));
                        }
                    }
                }
                deleteList.add(del);
            }
            table.delete(deleteList);
            table.close();
        }
    }

    /**
     * 根据RowKey获取数据
     *
     * @param tableName 表名称
     * @param rowKey    RowKey名称
     * @param colFamily 列族名称
     * @param col       列名称
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        if (colFamily != null) {
            get.addFamily(Bytes.toBytes(colFamily));
            if (col != null) {
                get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
            }
        }
        Result result = table.get(get);
        showCell(result);
        table.close();
    }

    /**
     * 根据RowKey获取信息
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getData(String tableName, String rowKey) throws IOException {
        getData(tableName, rowKey, null, null);
    }

    /**
     * 扫描表
     *
     * @param tableName
     * @throws IOException
     */
    public static List<HBaseResult> scanTable(String tableName, String start, String end, int size) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        Scan scan = new Scan();
        scan.setMaxResultSize(size);
        if (StringUtils.isNotBlank(start)) {
            scan.setStartRow(Bytes.toBytes(start));
        }
        if (StringUtils.isNotBlank(end)) {
            scan.setStopRow(Bytes.toBytes(end));
        }
        List<HBaseResult> results = new ArrayList<>();
        if (admin.tableExists(tName)) {
            Table table = connection.getTable(tName);
            ResultScanner scanner = table.getScanner(scan);
            try {
                for (Result result : scanner) {
                    results.add(getResult(result));
                }
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
                if (table != null) {
                    table.close();
                }
            }
        } else {
            log(tableName + " not exists.");
        }
        return results;
    }

    /**
     * 扫描表
     *
     * @param tableName
     * @throws IOException
     */
    public static List<HBaseResult> scanTable(String tableName) throws IOException {
        return scanTable(tableName, "", "", DEFAULT_SIZE);
    }

    /**
     * 分页的复合条件查询
     * @param tableName       表名
     * @param startKey start
     * @param endKey end
     * @param hbaseConditions 复合条件
     * @param size            每页显示的数量
     * @return
     */
    public static List<HBaseResult> scanByConditions(String tableName, String startKey, String endKey, int size,
                                                             List<HbaseConditionEntity> hbaseConditions) throws IOException {
        ResultScanner rs = null;
        Table table = null;
        TableName tName = TableName.valueOf(tableName);
        try {
            table = connection.getTable(tName);
            Scan scan = new Scan();
            if (StringUtils.isBlank(startKey)) {
                scan.setStartRow(Bytes.toBytes(startKey));
            }
            if (StringUtils.isBlank(endKey)) {
                scan.setStopRow(Bytes.toBytes(endKey));
            }
            scan.setMaxResultSize(size);
            scan.setReversed(true);
            // 过滤条件列表
            FilterList filterList = getFilterList(hbaseConditions);
            scan.setFilter(filterList);
            rs = table.getScanner(scan);
            List<HBaseResult> results = new ArrayList<>();
            for (Result r : rs) {
                results.add(getResult(r));
            }
            return results;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * 组装过滤条件
     * @param hbaseConditions 条件
     * @return
     */
    private static FilterList getFilterList(List<HbaseConditionEntity> hbaseConditions) {
        FilterList filterList = null;
        FilterList.Operator operator = FilterList.Operator.MUST_PASS_ALL;
        for (HbaseConditionEntity hbaseCondition : hbaseConditions) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    hbaseCondition.getFamilyColumn(),
                    hbaseCondition.getColumn(),
                    hbaseCondition.getCompareOp(),
                    hbaseCondition.getValue());
            filter.setFilterIfMissing(true);
            if (filterList == null) {
                if (hbaseCondition.getOperator() != null) {
                    operator = hbaseCondition.getOperator();
                }
                filterList = new FilterList(operator);
                filterList.addFilter(filter);
            } else {
                if (operator.equals(hbaseCondition.getOperator())) {
                    filterList.addFilter(filter);
                } else {
                    operator = hbaseCondition.getOperator();
                    FilterList newFilterList = new FilterList(operator);
                    newFilterList.addFilter(filterList);
                    newFilterList.addFilter(filter);
                    filterList = newFilterList;
                }
            }
        }
        return filterList;
    }

    /**
     * 格式化输出
     *
     * @param result
     */
    public static void showCell(Result result) {
        log(getResult(result));
    }

    /**
     * 处理结果
     *
     * @param result
     */
    public static HBaseResult getResult(Result result) {
        if (result == null) return null;
        Cell[] cells = result.rawCells();
        HBaseResult hBaseResult = new HBaseResult();
        Map<String, String> results = new HashMap<>(cells.length);
        for (int i = 0; i < cells.length; i++) {
            Cell cell = cells[i];
            if (i == 0) {
                hBaseResult.setKey(Bytes.toString(CellUtil.cloneRow(cell)));
                hBaseResult.setTimestamp(cell.getTimestamp());
                hBaseResult.setColumnFamily(Bytes.toString(CellUtil.cloneFamily(cell)));
            }
            String key = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            results.put(key, value);
        }
        hBaseResult.setResults(results);
        return hBaseResult;
    }

    public static void addTableCoprocessor(String tableName) {
        try {
            TableName tName = TableName.valueOf(tableName);
            if (admin.tableExists(tName)) {
                admin.disableTable(tName);
                HTableDescriptor htd = admin.getTableDescriptor(tName);
                htd.addCoprocessor(AggregateImplementation.class.getName());
                admin.modifyTable(tName, htd);
                admin.enableTable(tName);
            }
        } catch (IOException e) {
            LOG.error("hbase error!", e);
        }
    }

    /**
     * 多少行 尽量少用
     *
     * @param tableName 表
     * @param family    列族
     * @return
     */
    public static long rowCount(String tableName, String family) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        long rowCount = 0;
        try {
            rowCount = aggregationClient.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            LOG.error("hbase error!", e);
        }
        return rowCount;
    }

    /**
     * 打印
     *
     * @param obj 打印对象
     */
    private static void log(Object obj) {
        LOG.info(obj);
    }

    public static void main(String[] args) throws IOException {
        addTableCoprocessor("S54321");
        long l = rowCount("S54321", "info");
        System.out.println(l);
    }
} 
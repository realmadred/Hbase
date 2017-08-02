package com.hbase;

/**
 * Created by root on 2016/12/13.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseDemo {

    public static Configuration conf;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master");
        conf.addResource("hbase-site.xml");
    }

    /**
     * 创建表
     *
     * @param tablename    表名
     * @param columnFamily 列族
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     * @throws IOException
     */
    public static void createTable(String tablename, String columnFamily)
            throws MasterNotRunningException, IOException, ZooKeeperConnectionException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        try {
            if (admin.tableExists(TableName.valueOf(tablename))) {
                System.out.println("Create table data: " + tablename + " already exists, and drop it successfully.");
            } else {
                TableName tableName = TableName.valueOf(tablename);
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(tableDesc);
                System.out.println("Create table data: " + tablename + " created succeed.");
            }
        } finally {
            admin.close();
            conn.close();
        }
    }

    /**
     * 向表中插入一条新数据
     *
     * @param tableName    表名
     * @param row          行键key
     * @param columnFamily 列族
     * @param column       列名
     * @param data         要插入的数据
     * @throws IOException
     */
    public static void putData(String tableName, String row, String columnFamily, String column, String data)
            throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
            System.out.println("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'" + " successfully.");
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * add a column family to an existing table
     *
     * @param tableName    table name
     * @param columnFamily column family
     * @throws IOException
     */
    public static void putFamily(String tableName, String columnFamily) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        try {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                System.out.println(tableName + " not exists");
            } else {
                admin.disableTable(TableName.valueOf(tableName));

                HColumnDescriptor cf1 = new HColumnDescriptor(columnFamily);
                admin.addColumn(TableName.valueOf(tableName), cf1);

                admin.enableTable(TableName.valueOf(tableName));
                System.out.println("Put family data: " + TableName.valueOf(tableName) + ", " + columnFamily + " put succeed.");
            }
        } finally {
            admin.close();
            conn.close();
        }
    }

    /**
     * 根据key读取一条数据
     *
     * @param tableName    表名
     * @param row          行键key
     * @param columnFamily 列族
     * @param column       列名
     * @throws IOException
     */
    public static void getData(String tableName, String row, String columnFamily, String column) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            Get get = new Get(Bytes.toBytes(row));
            Result result = table.get(get);
            byte[] rb = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            String value = new String(rb, "UTF-8");
            System.out.println("Get data " + value + " successfully.");
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * get all data of a table
     *
     * @param tableName table name
     * @throws IOException
     */
    public static void scanAll(String tableName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    String row = new String(result.getRow(), "UTF-8");
                    String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
                    String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
                    String value = new String(CellUtil.cloneValue(cell), "UTF-8");
                    System.out.println("Scan all data: [行健row:" + row + "],[列簇family:" + family + "],[列qualifier:" + qualifier + "],[value值:" + value + "],[时间戳:" + cell.getTimestamp() + "] successfully.");
                }
            }
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * delete a data by row key
     *
     * @param tableName table name
     * @param rowKey    row key
     * @throws IOException
     */
    public static void delData(String tableName, String rowKey) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            List<Delete> list = new ArrayList<Delete>();
            Delete del = new Delete(rowKey.getBytes());
            list.add(del);
            table.delete(list);
            System.out.println("Delete record " + rowKey + " successfully.");
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * delete a column's value of a row
     *
     * @param tableName  table name
     * @param rowKey     row key
     * @param familyName family name
     * @param columnName column name
     * @throws IOException
     */
    public static void deleteColumn(String tableName, String rowKey, String familyName, String columnName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            Delete del = new Delete(Bytes.toBytes(rowKey));
            del.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            List<Delete> list = new ArrayList<Delete>(1);
            list.add(del);
            table.delete(list);
            System.out.println("Delete column data: [table:" + tableName + "],row:" + rowKey + "],[family:" + familyName + "],[qualifier:" + columnName + "] successfully.");
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * delete a columnFamily's all columns value of a row
     *
     * @param tableName  table name
     * @param rowKey     row key
     * @param familyName family name
     * @throws IOException
     */
    public static void deleteFamily(String tableName, String rowKey, String familyName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf(tableName));
        try {
            Delete del = new Delete(Bytes.toBytes(rowKey));
            del.addFamily(Bytes.toBytes(familyName));
            List<Delete> list = new ArrayList<Delete>(1);
            list.add(del);
            table.delete(list);
            System.out.println("Delete family data: [table:" + tableName + "],row:" + rowKey + "],[family:" + familyName + "] successfully.");
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * delete a table
     *
     * @param tableName table name
     * @throws IOException
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     */
    public static void deleteTable(String tableName) throws IOException, MasterNotRunningException, ZooKeeperConnectionException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("Delete table " + tableName + " ok.");
        } finally {
            admin.close();
            conn.close();
        }
    }

    public static void main(String[] args) {
        System.err.println("HBase test start...");


//      String tableName = "student";
        String tableName = "hpwy.studentinfo";
        try {
            deleteTable(tableName);
            createTable(tableName, "stu");
//          createTable(tableName + "2", "stu");
/*            createTable(tableName, "load");
            putData(tableName, "row_1", "load", "no", "0001");
            putData(tableName, "row_1", "load", "rec_date", "2016-06-03");
            putData(tableName, "row_1", "load", "rec_time", "09:49:00");
            putData(tableName, "row_1", "load", "power", "154.24");*/

            putData(tableName, "row_1", "stu", "stu_id", "001");
            putData(tableName, "row_2", "stu", "stu_id", "002");
            putData(tableName, "row_3", "stu", "stu_id", "003");
            getData(tableName, "row_1", "stu", "stu_id");
            delData(tableName, "row_3");
            scanAll(tableName);
//          deleteTable(tableName + "2");
//          putFamily(tableName, "score");
//          putData(tableName, "row_4", "score", "chinese", "90");
//          putData(tableName, "row_5", "score", "math", "91");
//          scanAll(tableName);
//          deleteColumn(tableName, "row_4", "score", "chinese");
//          deleteFamily(tableName, "row_5", "score");
//            scanAll(tableName);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.err.println("HBase test end...");
    }
}

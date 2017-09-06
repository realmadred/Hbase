package com.hbase;

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring.xml" })
public class JdbcTest {

    @Autowired
    private JdbcTemplate template;
    @Autowired
    private DataSource dataSource;

    private static final String PATTEN = "_";
    private static final String LINE = "\n";
    private static final String TAB = "\t";


    @Test
    public void test1(){
        List<Map<String, Object>> map = template.queryForList("SELECT * FROM x_car_run_log WHERE  id = 1", new Object[0]);
        System.out.println(map.size());
    }

    @Test
    public void getFields() throws Exception {
        String sql = "SELECT * FROM x_car_run_log LIMIT 1";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery()
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            if (count > 0) {
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < count; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    stringBuilder.append(columnName).append(" ")
                            .append(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName));
                    if (i<count-1){
                        stringBuilder.append(",");
                    }
                }
                System.out.println(stringBuilder);
                System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,stringBuilder.toString()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getDataPuts() throws Exception {
        String sql = "SELECT * FROM d_topic LIMIT 1";
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery()
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            if (count > 0) {
                for (int i = 0; i < count; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    if ("id".equals(columnName)) continue;
                    System.out.println("data.put(\""+ columnName +"\",1);");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createBean() throws Exception {
        String table = "d_topic";// 表名
        String sql = "SELECT * FROM "+table +" LIMIT 1";
        String sqlColumns = "show full columns from "+table;
        try (
                Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql);
                PreparedStatement psColumns = connection.prepareStatement(sqlColumns);
                ResultSet resultSet = ps.executeQuery();
                ResultSet resultSetColumns = psColumns.executeQuery()
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            String tableName = metaData.getTableName(1);
            tableName = getStr1(tableName,PATTEN);
//            tableName = tableName.substring(tableName.indexOf(PATTEN)+1);
            StringBuilder str = new StringBuilder("public class "
                    +StringUtils.capitalize(tableName.substring(1))
//					+ " extends BaseInputVO implements Serializable {" + LINE + LINE +
                    + " implements Serializable {" + LINE + LINE +
                    TAB + "private static final long serialVersionUID = "
                    + new Random().nextLong()+ "L;" + LINE + LINE) ;
            if (count > 0) {
                for (int i = 1; i <= count; i++) {
                    String columnName = metaData.getColumnName(i);
                    String typeName = metaData.getColumnTypeName(i);
                    String remarks = "";
                    if (resultSetColumns.next()) remarks =resultSetColumns.getString("Comment") ;
                    if ("INT".equalsIgnoreCase(typeName) || "TINYINT".equalsIgnoreCase(typeName)){
                        typeName = "Integer";
                    }else if ("VARCHAR".equalsIgnoreCase(typeName)){
                        typeName = "String";
                    }else if ("DATETIME".equalsIgnoreCase(typeName)){
                        typeName = "Date";
                    }else if ("DOUBLE".equalsIgnoreCase(typeName)){
                        typeName = "Double";
                    }
                    str.append(TAB + "private ").append(typeName+" ").append(getStr1(columnName, PATTEN)+" ;")
                            .append(" // "+remarks+ LINE);
                }
            }
            str.append("}" + LINE);
            System.out.println(str.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static String getStr(String str,String patten){
        if (StringUtils.isBlank(str) || patten == null){
            return "";
        }
        int i = str.indexOf(patten);
        if (i >=0){
            String substring = str.substring(i + patten.length());
            substring = StringUtils.capitalize(substring);
            str = str.substring(0,i)+ substring;
            return getStr(str,patten);
        }else {
            return str;
        }
    }

    public static String getStr1(String str,String patten){
        if (StringUtils.isBlank(str) || patten == null){
            return "";
        }
        StringTokenizer tokenizer = new StringTokenizer(str,patten);
        StringBuilder stringBuilder = new StringBuilder();
        int i=0;
        while (tokenizer.hasMoreElements()){
            String s = tokenizer.nextToken();
            if (i > 0){
                s = StringUtils.capitalize(s);
            }else {
                i = 1;
            }
            stringBuilder.append(s);
        }
        return stringBuilder.toString();
    }

    @Test
    public void getRemarks() throws Exception {
        try (
                Connection connection = dataSource.getConnection()
        ) {
            String tableName = "d_topic";

            DatabaseMetaData metaData = connection.getMetaData();
            String userName = metaData.getUserName().toUpperCase();
            ResultSet rs = metaData.getColumns(null, userName, tableName, "%");
            Map map = new HashMap();
            while(rs.next()){
                String colName = rs.getString("COLUMN_NAME");
                String remarks = rs.getString("REMARKS");
                if(remarks == null || remarks.equals("")){
                    remarks = colName;
                }
                map.put(colName,remarks);
            }
            System.out.println(map);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @org.junit.Test
    public void test() {
        final String s = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "total_pay_money");
        System.out.println(s);
        final StringBuilder stringBuilder = Joiner.on(",").appendTo(new StringBuilder(""), "ccc", "dd","aa");
        System.out.println(stringBuilder);
        final ArrayList<Integer> integers = Lists.newArrayList(1, 21, 2, 3, 5, 6);
        System.out.println(Joiner.on(",").join(integers));
    }

}

package com.hbase.entity;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * 条件对象
 * 2017-08-02 16:24:09
 */
public class HbaseConditionEntity implements Serializable{

    private static final long serialVersionUID = 7049975374458542521L;
    private byte[] familyColumn;
    private byte[] column;
    private byte[] value;
    private Operator operator = Operator.MUST_PASS_ALL;
    private CompareOp compareOp = CompareOp.EQUAL;

    public HbaseConditionEntity(byte[] familyColumn, byte[] column,
                                byte[] value, Operator operator, CompareOp compareOp) {
        this.familyColumn = familyColumn;
        this.column = column;
        this.value = value;
        setOperator(operator);
        setCompareOp(compareOp);
    }

    public HbaseConditionEntity(String familyColumn, String column,
                                String value, Operator operator, CompareOp compareOp) {
        this(Bytes.toBytes(familyColumn),Bytes.toBytes(column),Bytes.toBytes(value),operator,compareOp);
    }

    public HbaseConditionEntity(String familyColumn, String column,
                                String value) {
        this.familyColumn = Bytes.toBytes(familyColumn);
        this.column = Bytes.toBytes(column);
        this.value = Bytes.toBytes(value);
    }

    public HbaseConditionEntity() {
    }

    public byte[] getFamilyColumn() {
        return familyColumn;
    }

    public void setFamilyColumn(byte[] familyColumn) {
        this.familyColumn = familyColumn;
    }

    public byte[] getColumn() {
        return column;
    }

    public void setColumn(byte[] column) {
        this.column = column;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        if (operator == null) return;
        this.operator = operator;
    }

    public CompareOp getCompareOp() {
        return compareOp;
    }

    public void setCompareOp(CompareOp compareOp) {
        if (compareOp == null) return;
        this.compareOp = compareOp;
    }

    public static List<HbaseConditionEntity> toHbaseConditions(String labels) {
        List<HbaseConditionEntity> hbaseConditions = new ArrayList<HbaseConditionEntity>();
        String[] labelArray = labels.split(";");
        for (String labelWithCompares : labelArray) {
            String[] labelWithComparesArray = labelWithCompares.split(" ");
            String label = labelWithComparesArray[0];
            Operator compare = null;
            if (labelWithComparesArray.length > 1) {
                if ("and".equals(labelWithComparesArray[1])) {
                    compare = Operator.MUST_PASS_ALL;
                } else {
                    compare = Operator.MUST_PASS_ONE;
                }
            }
            byte[] familyColumn = Bytes.toBytes("label");
            byte[] column = Bytes.toBytes(label);
            byte[] value = Bytes.toBytes(label);
            HbaseConditionEntity hbaseCondition = new HbaseConditionEntity(
                    familyColumn, column, value, compare, CompareOp.EQUAL);
            hbaseConditions.add(hbaseCondition);
        }
        return hbaseConditions;
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        String labels = "label1 and;label2 or;label3";
        List<HbaseConditionEntity> conditions = toHbaseConditions(labels);
        System.out.println("==========begin==========");

        for (HbaseConditionEntity hbaseConditionEntity : conditions) {
            System.out.println("[familyColumn: " + new String(hbaseConditionEntity.getFamilyColumn(), "UTF-8")
                    + "] [column: " + new String(hbaseConditionEntity.getColumn(), "UTF-8")
                    + "] [value: " + new String(hbaseConditionEntity.getValue(), "UTF-8")
                    + "] [operator: " + hbaseConditionEntity.getOperator()
                    + "] [compare: " + hbaseConditionEntity.getCompareOp() + "]");
        }

        System.out.println("==========end==========");
    }

}


package com.hbase.entity.jdbc;

public class ColumnOps {

    private String column;
    private CompareOp compareOp = CompareOp.EQUAL;

    public ColumnOps() {
    }

    public ColumnOps(String column, CompareOp compareOp) {
        this.column = column;
        this.compareOp = compareOp;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public void setCompareOp(CompareOp compareOp) {
        this.compareOp = compareOp;
    }

    @Override
    public String toString() {
        return column + compareOp.value;
    }

    public enum CompareOp {
        LESS(" < ? "),
        LESS_OR_EQUAL(" <= ? "),
        EQUAL(" = ? "),
        NOT_EQUAL(" <> ? "),
        GREATER_OR_EQUAL(" >= ? "),
        GREATER(" > ? "),
        LIKE(" LIKE ? ");
        String value;
        CompareOp(String value) {
            this.value = value;
        }
    }
}

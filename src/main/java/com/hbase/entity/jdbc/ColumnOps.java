package com.hbase.entity.jdbc;

public class ColumnOps {

    private String column;
    private Compare compare = Compare.EQUAL;

    public ColumnOps() {
    }

    public ColumnOps(String column, Compare compare) {
        this.column = column;
        this.compare = compare;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public void setCompare(Compare compare) {
        this.compare = compare;
    }

    @Override
    public String toString() {
        return column + compare.value;
    }

}

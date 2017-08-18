package com.hbase.test.entity;

/**
 * @auther Administrator
 * @date 2017/8/17
 * @description 描述
 */
public class Page extends BaseEntity{

    private Integer start;
    private Integer end;

    public Integer getStart() {
        return start;
    }

    public void setStart(final Integer start) {
        this.start = start;
    }

    public Integer getEnd() {
        return end;
    }

    public void setEnd(final Integer end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "Page{" +
                "start=" + start +
                ", end=" + end +
                "} " + super.toString();
    }
}

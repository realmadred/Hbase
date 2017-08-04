package com.hbase.entity.jdbc;

/**
 * 分页实体类
 * lf
 * 2017-08-03 09:58:43
 */
public class PageCondition {

    private final static int DEFAULT_SIZE = 10;
    private final static int DEFAULT_PAGE = 1;
    private int page = DEFAULT_PAGE;
    private int size = DEFAULT_SIZE;

    public PageCondition() {
    }

    public PageCondition(int page, int size) {
        setSize(size);
        setPage(page);
    }

    public int getSize() {
        return size;
    }

    public PageCondition setSize(int size) {
        if (size <= 0 || size > 100) {
            return this;
        }
        this.size = size;
        return this;
    }

    public int getPage() {
        return page;
    }

    public PageCondition setPage(int page) {
        this.page = page <= 0 ? DEFAULT_PAGE : page;
        return this;
    }

    /**
     * 获取起始行
     * lf
     * 2017-08-03 10:12:47
     *
     * @return
     */
    public int getStart() {
        return size * (page - 1);
    }

}

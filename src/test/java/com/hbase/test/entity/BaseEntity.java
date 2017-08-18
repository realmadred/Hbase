package com.hbase.test.entity;

/**
 * @auther Administrator
 * @date 2017/8/17
 * @description 描述
 */
public class BaseEntity {

    private Integer id;

    public Integer getId() {
        return id;
    }

    public void setId(final Integer id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "BaseEntity{" +
                "id=" + id +
                '}';
    }
}

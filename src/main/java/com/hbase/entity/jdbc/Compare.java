package com.hbase.entity.jdbc;

public enum Compare {
        LESS(" < ? "),
        LESS_OR_EQUAL(" <= ? "),
        EQUAL(" = ? "),
        NOT_EQUAL(" <> ? "),
        GREATER_OR_EQUAL(" >= ? "),
        GREATER(" > ? "),
        LIKE(" LIKE ? ");
        String value;
        Compare(String value) {
            this.value = value;
        }
    }
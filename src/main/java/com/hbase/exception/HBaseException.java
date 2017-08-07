package com.hbase.exception;

/**
 * hbase 操作异常
 * lf
 * 2017-08-07 13:46:51
 */
public class HBaseException extends RuntimeException {

    public HBaseException() {
    }

    public HBaseException(String message) {
        super(message);
    }

    public HBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public HBaseException(Throwable cause) {
        super(cause);
    }

    public HBaseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

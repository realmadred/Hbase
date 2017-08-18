package com.hbase.exception;

/**
 * @auther Administrator
 * @date 2017/8/17
 * @description 描述
 */
public class DaoException extends RuntimeException {

    public DaoException() {
    }

    public DaoException(final String message) {
        super(message);
    }

    public DaoException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DaoException(final Throwable cause) {
        super(cause);
    }

    public DaoException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

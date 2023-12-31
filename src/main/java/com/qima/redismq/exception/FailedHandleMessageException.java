package com.qima.redismq.exception;

public class FailedHandleMessageException extends Exception{

    public FailedHandleMessageException(String message) {
        super(message);
    }

    public FailedHandleMessageException(Throwable cause) {
        super(cause);
    }

    public FailedHandleMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}

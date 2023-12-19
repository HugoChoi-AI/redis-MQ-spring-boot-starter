package com.qima.redismq.exception;

public class ServiceNotReachableException extends Throwable{

    public ServiceNotReachableException(String message) {
        super(message);
    }

    public ServiceNotReachableException(Throwable cause) {
        super(cause);
    }

    public ServiceNotReachableException(String message, Throwable cause) {
        super(message, cause);
    }
}

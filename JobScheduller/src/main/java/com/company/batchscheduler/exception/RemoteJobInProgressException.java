package com.company.batchscheduler.exception;

public class RemoteJobInProgressException extends Exception {

    public RemoteJobInProgressException(String error) {
        super(error);
    }

}

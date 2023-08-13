package com.klimber.hiveddbudtf.client.ddb;

public class DynamoDbClientWrapperException extends RuntimeException{
    
    public DynamoDbClientWrapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
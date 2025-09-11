package com.amazonaws.neptune.client.jena;

public class NeptuneJenaConnectionException extends RuntimeException {
    public NeptuneJenaConnectionException(String message) {
        super(message);
    }
    public NeptuneJenaConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}

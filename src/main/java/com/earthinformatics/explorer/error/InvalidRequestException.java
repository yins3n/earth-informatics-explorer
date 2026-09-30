package com.earthinformatics.explorer.error;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * Raised when a caller supplies a parameter the service cannot honour (bad bbox, grid step out
 * of range, unknown feed). Surfaces as HTTP 400 with the offending field named in the message.
 */
public class InvalidRequestException extends ResponseStatusException {

    public InvalidRequestException(String message) {
        super(HttpStatus.BAD_REQUEST, message);
    }

    public InvalidRequestException(String message, Throwable cause) {
        super(HttpStatus.BAD_REQUEST, message, cause);
    }
}

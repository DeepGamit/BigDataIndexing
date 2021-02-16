package com.bigdataindexing.project.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.ALREADY_REPORTED)
public class PlanAlreadyPresentException extends RuntimeException {
    public PlanAlreadyPresentException(String message) {
        super(message);
    }
}

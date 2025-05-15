package org.distributed.shardingjh.common.response;

import lombok.AllArgsConstructor;

/**
 * Mapping table for error response codes and messages
 *
 * @author chris
 * */
@AllArgsConstructor
public enum MgrResponseCode {
    SUCCESS("0000", "Success"),

    INVALID_REQUEST("0001", "Invalid request"),
    INVALID_REMOTE_API("0002", "Remote call exception"),
    REQUEST_ACCESS_DENIED("003", "Unauthorized access request, access denied"),
    TOO_MANY_REQUESTS("004", "Too Many Requests"),

    PARAM_NOT_FOUND("0101", "Parameter not found"),
    PARAM_INVALID("0102", "Invalid parameter"),

    MEMBER_NOT_FOUND("0201", "User not found"),
    MEMBER_ALREADY_EXISTS("0202", "User already exists"),
    MEMBER_PASSWORD_INVALID("0203", "Invalid username or password"),
    MEMBER_DISABLED("0204", "User is disabled"),

    DB_FAIL("0401", "Database operation failed"),

    UNKNOWN_ERROR("9999", "System error");

    private String code;
    private String message;

    public String getCode() {
        return this.code;
    }

    public String getMessage() {
        return this.message;
    }
}
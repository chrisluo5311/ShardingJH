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
    UNSUPPORTED_OPERATION("005", "Unsupported operation"),
    SERVER_ERROR("006", "Server error"),
    UNAUTHORIZED_REQUEST("007", "Unauthorized request"),
    JSON_PARSE_ERROR("008", "JSON parse error"),

    PARAM_NOT_FOUND("0101", "Parameter not found"),
    PARAM_INVALID("0102", "Invalid parameter"),

    MEMBER_NOT_FOUND("0201", "Member not found"),
    MEMBER_ALREADY_EXISTS("0202", "Member already exists"),
    MEMBER_NAME_INVALID("0203", "Invalid Member Name"),
    MEMBER_DISABLED("0204", "Member is disabled"),

    ORDER_NOT_FOUND("0301", "Order not found"),
    ORDER_ALREADY_EXISTS("0302", "Order already exists"),

    DB_FAIL("0401", "Database operation failed"),
    DB_CONFLICT("0402", "Database conflict"),

    PRODUCT_NOT_FOUND("0501", "Product not found"),

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
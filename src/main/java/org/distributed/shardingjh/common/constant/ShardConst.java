package org.distributed.shardingjh.common.constant;

/**
 * ShardConst
 *
 * @author Chris
 */
public class ShardConst {

    // Shard for regular users
    public static final Integer TOTAL_SHARD_COMMON_COUNT = 2;
    //Common shard prefix
    public static final String SHARD_COMMON_PREFIX = "NORMAL_";
    // Shard for orders
    public static final String SHARD_ORDER_PREFIX = "ORDER_";
    // Shard for old orders
    public static final String SHARD_ORDER_OLD = "OLD";
    // Total server count
    public static final Integer TOTAL_SERVER_COUNT = 3;
}

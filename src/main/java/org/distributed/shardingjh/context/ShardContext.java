package org.distributed.shardingjh.context;

/*
* Uses ThreadLocal<String> to store the current shard key.
* @author chris
*/
public class ShardContext {
    private static final ThreadLocal<String> context = new ThreadLocal<>();

    // Before any DB operation, set the shard key
    // Default is "shard1"
    public static void setCurrentShard(String shardKey) {
        context.set(shardKey);
    }

    // Get the current shard key for the current thread/request
    public static String getCurrentShard() {
        return context.get();
    }

    // Clear the shard key after the DB operation is done
    public static void clear() {
        context.remove();
    }
}

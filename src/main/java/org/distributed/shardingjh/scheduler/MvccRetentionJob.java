package org.distributed.shardingjh.scheduler;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MvccRetentionJob {

    @Resource(name = "order2025JdbcTemplate")
    private JdbcTemplate jdbc2025;

    @Resource(name = "order2024JdbcTemplate")
    private JdbcTemplate jdbc2024;

    @Resource(name = "orderOldJdbcTemplate")
    private JdbcTemplate jdbcOld;

    @Scheduled(cron = "0 0 2 * * ?")
    public void purgeAll() {
        purge(jdbc2025, "shard_order_2025");
        purge(jdbc2024, "shard_order_2024");
        purge(jdbcOld, "shard_order_old");
    }

    public void purge(JdbcTemplate jdbc, String label) {
        int deleted = jdbc.update("""
            DELETE FROM order_table
            WHERE expired_at IS NOT NULL
            AND expired_at < datetime('now', '-2 days')
        """);
        log.info("🧹[{}] Purged {} expired MVCC order versions older than 3 days.", label ,deleted);
    }

}

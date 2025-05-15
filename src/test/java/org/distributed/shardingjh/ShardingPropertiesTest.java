package org.distributed.shardingjh;

import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.config.ShardingProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Map;

@Slf4j
@SpringBootTest
@ActiveProfiles("test") // Use the 'test' profile to load properties from application-test.yml
class ShardingPropertiesTest {

    @Autowired
    private ShardingProperties shardingProperties;

    /**
     * Test the binding of sharding properties from the configuration file.
     * This test checks if the properties are correctly loaded into the ShardingProperties class.
     */
    @Test
    void testShardingPropertiesBinding() {
        // Verify that the lookup map is populated from the test configuration
        Map<String, String> lookup = shardingProperties.getLookup();
        log.info("Sharding lookup: {}", lookup);
        assertThat(lookup).isNotNull();
        assertThat(lookup).containsEntry("NORMAL_1", "shard_common_1.db");
        assertThat(lookup).containsEntry("NORMAL_2", "shard_common_2.db");
    }
}
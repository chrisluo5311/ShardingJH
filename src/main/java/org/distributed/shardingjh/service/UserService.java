package org.distributed.shardingjh.service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.User;
import org.distributed.shardingjh.repository.UserRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class UserService {

    @Resource
    private UserRepository userRepository;

    @Resource
    private RedisTemplate<String, User> redisTemplate;

    private final String REDIS_KEY_PREFIX = "USER_";

    public User findById(Long id) {
        try {
            // Route to the appropriate shard based on ID
            routeShard(id);
            // Check Redis cache first
            String key = REDIS_KEY_PREFIX + id;
            log.info("Redis key: {}", key);
            User cahcedUser = redisTemplate.opsForValue().get(key);
            if (cahcedUser != null) return cahcedUser;
            Optional<User> user = userRepository.findById(id);
            user.ifPresent(u -> redisTemplate.opsForValue().set(key, u));
            return user.orElse(null);
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    public void saveUser(User user) {
        try {
            // Route to the appropriate shard based on ID
            routeShard(user.getId());
            String key = REDIS_KEY_PREFIX + user.getId();
            redisTemplate.opsForValue().set(key, user);
            userRepository.save(user);
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }

    }

    private void routeShard(Long id) {
        if (id % 2 == 0) {
            // Route to shard 2
            ShardContext.setCurrentShard("shard2");
            log.info("ID {} routed to shard 2", id);
            log.info("ShardContext.getCurrentShard() = {}", ShardContext.getCurrentShard());
        } else {
            // Route to shard 1
            ShardContext.setCurrentShard("shard1");
            log.info("ID {} routed to shard 1", id);
        }
    }
}

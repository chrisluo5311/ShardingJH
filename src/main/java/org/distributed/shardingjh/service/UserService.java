package org.distributed.shardingjh.service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
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
        String key = REDIS_KEY_PREFIX + id;
        log.info("Redis key: {}", key);
        User cahcedUser = redisTemplate.opsForValue().get(key);
        if (cahcedUser != null) return cahcedUser;
        Optional<User> user = userRepository.findById(id);
        user.ifPresent(u -> redisTemplate.opsForValue().set(key, u));
        return user.orElse(null);
    }

    public void saveUser(User user) {
        String key = REDIS_KEY_PREFIX + user.getId();
        redisTemplate.opsForValue().set(key, user);
        userRepository.save(user);
    }
}

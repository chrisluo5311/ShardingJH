package org.distributed.shardingjh.service;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.RedisConst;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.repository.user.MemberRepository;
import org.distributed.shardingjh.sharding.Impl.HashStrategy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class MemberService {

    @Resource
    private MemberRepository memberRepository;

    @Resource
    private RedisTemplate<String, Member> redisTemplate;

    @Resource
    private HashStrategy hashStrategy;

    /**
     * Search for a user by id
     * 1. Check Redis cache first
     * 2. If not found, search in the database
     * */
    public Member findById(String id) {
        try {
            log.info("Find Member by id: {}", id);
            // Check Redis cache first
            String key = RedisConst.REDIS_KEY_PREFIX + id;
            log.info("Member Redis key: {}", key);
            Member cahcedMember = redisTemplate.opsForValue().get(key);
            if (cahcedMember != null) return cahcedMember;

            // search in the database
            String shardKey = hashStrategy.resolveShard(id);
            ShardContext.setCurrentShard(shardKey);
            log.info("Member {} routing to {}", id, shardKey);
            Optional<Member> user = memberRepository.findById(id);
            user.ifPresent(u -> redisTemplate.opsForValue().set(key, u));
            return user.orElse(null);
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    /**
     * Route to the appropriate shard based on
     * 1. id => shard_common
     * */
    public Member saveUser(Member member) {
        try {
            log.info("Saving Member... : {}", member);
            String shardKey = hashStrategy.resolveShard(member.getId());
            log.info("Member {} routing to {}", member.getName(), shardKey);
            ShardContext.setCurrentShard(shardKey);
            String key = RedisConst.REDIS_KEY_PREFIX + member.getId();
            redisTemplate.opsForValue().set(key, member);
            memberRepository.save(member);
            return member;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }
}

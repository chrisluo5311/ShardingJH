package org.distributed.shardingjh.service.Impl;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.config.ShardingProperties;
import org.distributed.shardingjh.context.ShardContext;
import org.distributed.shardingjh.model.Member;
import org.distributed.shardingjh.repository.user.MemberRepository;
import org.distributed.shardingjh.service.MemberService;
import org.distributed.shardingjh.sharding.Impl.HashStrategy;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class MemberServiceImpl implements MemberService {

    @Resource
    private MemberRepository memberRepository;

    @Resource
    private HashStrategy hashStrategy;

    @Resource
    private ShardingProperties shardingProperties;

    /**
     * Search for a user by id
     * 1. Check Redis cache first
     * 2. If not found, search in the database
     * */
    @Override
    public Member findById(String id) {
        try {
            log.info("Find Member by id: {}", id);

            // search in the database
            String shardKey = hashStrategy.resolveShard(id);
            ShardContext.setCurrentShard(shardKey);
            log.info("Member {} routing to {}", id, shardKey);
            Optional<Member> user = memberRepository.findById(id);
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
    @Override
    public Member saveMember(Member member) {
        try {
            String shardKey = hashStrategy.resolveShard(member.getId());
            log.info("Routing member {} to shard {}", member.getId(), shardKey);
            ShardContext.setCurrentShard(shardKey);
            memberRepository.save(member);
            return member;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    /**
     * Find all members
     * */
    @Override
    public List<Member> findAllMembers() {
        try {
            List<Member> members = new ArrayList<>();
            log.info("Find all members");
            // Check all the shards
            for (int i = 1; i <= ShardConst.TOTAL_SHARD_COMMON_COUNT; i++) {
                String shardKey = shardingProperties.getLookup().get(ShardConst.SHARD_COMMON_PREFIX + i);
                log.info("Routing to shard {}", shardKey);
                ShardContext.setCurrentShard(shardKey);
                List<Member> shardMembers = memberRepository.findAll();
                members.addAll(shardMembers);
                ShardContext.clear();
            }

            return members;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }

    @Override
    public Member updateMember(Member member) {
        try {
            log.info("Update Member id: {}", member.getId());
            String shardKey = hashStrategy.resolveShard(member.getId());
            log.info("Member {} routing to {}", member.getId(), shardKey);
            ShardContext.setCurrentShard(shardKey);
            memberRepository.save(member);
            return member;
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }


    @Override
    public void deleteMember(String id) {
        try {
            // find shard of the user
            String shardKey = hashStrategy.resolveShard(id);
            log.info("Member {} routing to {}", id, shardKey);
            ShardContext.setCurrentShard(shardKey);
            // delete from database
            memberRepository.deleteById(id);
        } finally {
            // Clear the shard context after use
            ShardContext.clear();
        }
    }
}

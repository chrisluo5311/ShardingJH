package org.distributed.shardingjh.service;

import org.distributed.shardingjh.model.Member;

import java.util.List;

public interface MemberService {
    /**
     * Search for a user by id
     * 1. Check Redis cache first
     * 2. If not found, search in the database
     * */
    Member findById(String id);

    /**
     * Route to the appropriate shard based on
     * 1. id => shard_common
     * */
    Member saveMember(Member member);

    /**
     * Find all users
     * */
    List<Member> findAllMembers();


    /**
     * Update user information
     * */
    Member updateMember(Member member);

    /**
     * Delete user by id
     * */
    void deleteMember(String id);
}

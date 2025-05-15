package org.distributed.shardingjh.repository.user;

import org.distributed.shardingjh.model.Member;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface MemberRepository extends JpaRepository<Member, String> {

    Optional<Member> findByName(String userName);
}

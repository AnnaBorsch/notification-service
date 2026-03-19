package com.borscheva.spring.rest.notificationservice.repository;

import com.borscheva.spring.rest.notificationservice.entity.PushInbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface PushInboxRepository extends JpaRepository<PushInbox, UUID> {

    @Query(value = """
        SELECT * FROM push_inbox
        WHERE processed = false
        ORDER BY created_at ASC
        LIMIT :limit
    """, nativeQuery = true)
    List<PushInbox> findBatch(@Param("limit") int limit);
}
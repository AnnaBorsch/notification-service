package com.borscheva.spring.rest.notificationservice.repository;

import com.borscheva.spring.rest.notificationservice.entity.SmsInbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface SmsInboxRepository extends JpaRepository<SmsInbox, UUID> {

    @Query(value = """
        SELECT * FROM sms_inbox
        WHERE processed = false
        ORDER BY created_at ASC
        LIMIT :limit
    """, nativeQuery = true)
    List<SmsInbox> findBatch(@Param("limit") int limit);
}
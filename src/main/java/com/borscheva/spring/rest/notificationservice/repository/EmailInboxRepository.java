package com.borscheva.spring.rest.notificationservice.repository;

import com.borscheva.spring.rest.notificationservice.entity.EmailInbox;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface EmailInboxRepository extends JpaRepository<EmailInbox, UUID> {

    @Query(value = """
                SELECT * FROM email_inbox
                WHERE processed = false
                ORDER BY created_at ASC
                LIMIT :limit
            """, nativeQuery = true)
    List<EmailInbox> findBatch(@Param("limit") int limit);
}
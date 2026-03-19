package com.borscheva.spring.rest.notificationservice.scheduler;

import com.borscheva.spring.rest.notificationservice.entity.EmailInbox;
import com.borscheva.spring.rest.notificationservice.repository.EmailInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailInboxScheduler {

    private final EmailInboxRepository repository;

    @Value("${inbox.batch-size}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void process() {
        List<EmailInbox> batch = repository.findBatch(batchSize);

        for (EmailInbox msg : batch) {
            try {
                log.info("Обработка EMAIL: key={}, value={}", msg.getKey(), msg.getValue());
                msg.setProcessed(true);

            } catch (Exception e) {
                msg.setAttempt(msg.getAttempt() + 1);
                log.error("Ошибка обработки EMAIL {}: {}", msg.getId(), e.getMessage());
            }
        }
        repository.saveAll(batch);
    }
}
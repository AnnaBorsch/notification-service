package com.borscheva.spring.rest.notificationservice.scheduler;

import com.borscheva.spring.rest.notificationservice.entity.PushInbox;
import com.borscheva.spring.rest.notificationservice.repository.PushInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PushInboxScheduler {

    private final PushInboxRepository repository;

    @Value("${inbox.batch-size}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void process() {
        List<PushInbox> batch = repository.findBatch(batchSize);

        for (PushInbox msg : batch) {
            try {
                log.info("Обработка PUSH: key={}, value={}", msg.getKey(), msg.getValue());
                msg.setProcessed(true);

            } catch (Exception e) {
                msg.setAttempt(msg.getAttempt() + 1);
                log.error("Ошибка обработки PUSH {}: {}", msg.getId(), e.getMessage());
            }
        }
        repository.saveAll(batch);
    }
}
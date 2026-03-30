package com.borscheva.spring.rest.notificationservice.scheduler;

import com.borscheva.spring.rest.notificationservice.entity.TelegramInbox;
import com.borscheva.spring.rest.notificationservice.repository.TelegramInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class TelegramInboxScheduler {

    private final TelegramInboxRepository repository;

    @Value("${inbox.batch-size}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${inbox.delay-ms}")
    public void process() {
        List<TelegramInbox> batch = repository.findBatch(batchSize);

        for (TelegramInbox msg : batch) {
            try {
                log.info("Обработка TELEGRAM: key={}, value={}", msg.getKey(), msg.getValue());
                msg.setProcessed(true);

            } catch (Exception e) {
                msg.setAttempt(msg.getAttempt() + 1);
                log.error("Ошибка обработки TELEGRAM {}: {}", msg.getId(), e.getMessage());
            }
        }
        repository.saveAll(batch);
    }
}
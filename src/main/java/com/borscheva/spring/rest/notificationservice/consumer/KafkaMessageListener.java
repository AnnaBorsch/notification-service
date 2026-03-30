package com.borscheva.spring.rest.notificationservice.consumer;

import com.borscheva.spring.rest.notificationservice.entity.EmailInbox;
import com.borscheva.spring.rest.notificationservice.entity.PushInbox;
import com.borscheva.spring.rest.notificationservice.entity.SmsInbox;
import com.borscheva.spring.rest.notificationservice.entity.TelegramInbox;
import com.borscheva.spring.rest.notificationservice.repository.EmailInboxRepository;
import com.borscheva.spring.rest.notificationservice.repository.PushInboxRepository;
import com.borscheva.spring.rest.notificationservice.repository.SmsInboxRepository;
import com.borscheva.spring.rest.notificationservice.repository.TelegramInboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageListener {

    private final EmailInboxRepository emailInboxRepository;
    private final SmsInboxRepository smsInboxRepository;
    private final PushInboxRepository pushInboxRepository;
    private final TelegramInboxRepository telegramInboxRepository;

    @KafkaListener(topics = "email-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handleEmail(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();

        log.info("Получено email сообщение с key: {}, value: {}", key, message);

        EmailInbox inboxMessage = EmailInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("email-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        emailInboxRepository.save(inboxMessage);
        log.info("Email сообщение сохранено в inbox с key: {}", key);
    }

    @KafkaListener(topics = "sms-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handleSms(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();

        log.info("Получено sms сообщение с key: {}, value: {}", key, message);

        SmsInbox inboxMessage = SmsInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("sms-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        smsInboxRepository.save(inboxMessage);
        log.info("SMS сообщение сохранено в inbox с key: {}", key);
    }

    @KafkaListener(topics = "push-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handlePush(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();

        log.info("Получено push сообщение с key: {}, value: {}", key, message);

        PushInbox inboxMessage = PushInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("push-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        pushInboxRepository.save(inboxMessage);
        log.info("Push сообщение сохранено в inbox с key: {}", key);
    }

    @KafkaListener(topics = "telegram-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handleTelegram(ConsumerRecord<String, String> record) {
        String key = record.key();
        String message = record.value();

        log.info("Получено telegram сообщение с key: {}, value: {}", key, message);

        TelegramInbox inboxMessage = TelegramInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("telegram-events")
                .key(key)
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        telegramInboxRepository.save(inboxMessage);
        log.info("Telegram сообщение сохранено в inbox с key: {}", key);
    }
}
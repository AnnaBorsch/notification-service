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
    public void handleEmail(String message) {
        log.info("Получено email сообщение: {}", message);

        EmailInbox inboxMessage = EmailInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("email-events")
                .key(UUID.randomUUID().toString())
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        emailInboxRepository.save(inboxMessage);
        log.info("Email сообщение сохранено в inbox");
    }

    @KafkaListener(topics = "sms-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handleSms(String message) {
        log.info("Получено sms сообщение: {}", message);

        SmsInbox inboxMessage = SmsInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("sms-events")
                .key(UUID.randomUUID().toString())
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        smsInboxRepository.save(inboxMessage);
        log.info("SMS сообщение сохранено в inbox");
    }

    @KafkaListener(topics = "push-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handlePush(String message) {
        log.info("Получено push сообщение: {}", message);

        PushInbox inboxMessage = PushInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("push-events")
                .key(UUID.randomUUID().toString())
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        pushInboxRepository.save(inboxMessage);
        log.info("Push сообщение сохранено в inbox");
    }

    @KafkaListener(topics = "telegram-events", groupId = "${spring.kafka.consumer.group-id}")
    public void handleTelegram(String message) {
        log.info("Получено telegram сообщение: {}", message);

        TelegramInbox inboxMessage = TelegramInbox.builder()
                .id(UUID.randomUUID())
                .createdAt(LocalDateTime.now())
                .topic("telegram-events")
                .key(UUID.randomUUID().toString())
                .value(message)
                .processed(false)
                .attempt(1)
                .build();

        telegramInboxRepository.save(inboxMessage);
        log.info("Telegram сообщение сохранено в inbox");
    }
}
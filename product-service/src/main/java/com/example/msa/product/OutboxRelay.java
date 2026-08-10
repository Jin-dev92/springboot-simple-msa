package com.example.msa.product;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * outbox 테이블에 쌓인 메시지를 실제로 Kafka 에 내보낸다.
 *
 * <p>쓰는 쪽(오케스트레이터)과 보내는 쪽(여기)을 나눈 것이 Outbox 패턴의 전부다.
 * 쓰는 쪽은 자기 DB 트랜잭션 안에서 한 줄 넣고 끝내므로 브로커의 사정을 알 필요가
 * 없다. 브로커가 죽어 있어도 재고 차감은 정상적으로 커밋된다.
 *
 * <p>CDC(Debezium 등)로 DB 변경 로그를 직접 빨아 발행하는 방식이 실무의 정석이지만,
 * 여기서는 스케줄러 폴링을 쓴다. 이미 {@code SagaTimeoutSweeper}(order-service) 가 같은 모양이라
 * 새로 배울 인프라가 없고, Outbox 의 핵심인 <b>"상태와 메시지를 한 트랜잭션에 쓴다"</b>
 * 에 집중할 수 있기 때문이다. 대가는 폴링 주기만큼의 지연이다.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafkaTemplate;

    OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> outboxKafkaTemplate) {
        this.outbox = outbox;
        this.kafkaTemplate = outboxKafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval}")
    void publishPending() {
        List<OutboxMessage> pending = outbox.findTop100ByPublishedAtIsNullOrderByIdAsc();
        if (pending.isEmpty()) {
            return;
        }

        int sent = 0;
        for (OutboxMessage message : pending) {
            try {
                // 전송이 끝날 때까지 기다린 뒤에 발행 표시를 남긴다.
                // 비동기로 던져 놓고 바로 표시하면, 전송이 나중에 실패해도
                // 이미 "보냈다"고 기록되어 메시지가 사라진다.
                kafkaTemplate.send(message.getTopic(), message.getMessageKey(),
                        message.getPayload()).get(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // 인터럽트는 플래그를 되살려 두어야 스케줄러가 종료 신호를 알아챈다.
                Thread.currentThread().interrupt();
                log.warn("outbox 발행 중 인터럽트. 이번 주기를 중단한다 (id={})", message.getId());
                break;
            } catch (Exception e) {
                // 여기서 멈춘다. 다음 것으로 건너뛰면 순서가 깨지기 때문이다.
                // 같은 상품의 변경 순서가 뒤바뀌면 복제본이 낡은 값으로 덮인다.
                // 남은 것은 다음 주기가 이 자리부터 다시 이어 간다.
                log.warn("outbox 발행 실패. 순서를 지키기 위해 이번 주기를 여기서 멈춘다 "
                        + "(id={}, topic={}, 이번 주기 발행 {}건)",
                        message.getId(), message.getTopic(), sent, e);
                break;
            }

            message.markPublished();
            outbox.save(message);
            sent++;
        }

        if (sent > 0) {
            log.debug("outbox {}건 발행", sent);
        }
    }
}

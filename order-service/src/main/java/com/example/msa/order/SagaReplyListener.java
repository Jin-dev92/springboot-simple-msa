package com.example.msa.order;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 참여자들의 응답을 받아 오케스트레이터에 넘긴다. {@code StockResultListener} 를 대체한다.
 *
 * <p>이 클래스에는 판단이 없다. 받아서 넘기는 것이 전부다. 흐름 결정을 전부
 * 오케스트레이터에 두어야 "흐름이 한 파일에 모인다"는 이 전환의 목적이 지켜진다.
 *
 * <p>트랜잭션 경계가 오케스트레이터 쪽에 있는 것도 이 분리 덕이다.
 * {@code @Transactional} 은 스프링 프록시를 거쳐야 동작하므로, 같은 클래스 안에서
 * 자기 메서드를 부르면 트랜잭션이 조용히 걸리지 않는다(self-invocation).
 */
@Component
class SagaReplyListener {

    private final OrderSagaOrchestrator orchestrator;

    SagaReplyListener(OrderSagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topics = SagaReply.TOPIC, groupId = "order-service")
    void handle(SagaReply reply) {
        orchestrator.onReply(reply);
    }
}

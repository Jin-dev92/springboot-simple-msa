package com.example.msa.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 재고 처리 결과를 듣고 주문을 확정하거나 취소한다. Saga 의 마지막 구간이다.
 *
 * <p>주문을 시작한 서비스가 결과도 받아 마무리한다. 이렇게 시작한 쪽이 흐름을
 * 조율하는 방식을 코레오그래피(choreography) 라고 한다. 중앙에 흐름을 지시하는
 * 조정자를 두는 오케스트레이션(orchestration) 방식도 있는데, 참여 서비스가
 * 둘뿐인 지금은 조정자를 둘 이유가 없다.
 */
@Component
class StockResultListener {

    private static final Logger log = LoggerFactory.getLogger(StockResultListener.class);

    private final OrderRepository repository;

    StockResultListener(OrderRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = StockResultEvent.TOPIC, groupId = "order-service")
    void handle(StockResultEvent event) {
        repository.findById(event.orderId()).ifPresentOrElse(order -> {
            if (event.reserved()) {
                order.confirm();
                log.info("주문 확정: orderId={}", event.orderId());
            } else {
                // 보상. 이미 저장된 주문을 삭제하지 않고 취소 상태로 남긴다.
                // 왜 취소됐는지가 사용자에게도 운영자에게도 필요한 정보이기 때문이다.
                order.cancel(event.reason());
                log.warn("주문 취소(보상): orderId={}, 사유={}", event.orderId(), event.reason());
            }
            repository.save(order);
        }, () -> log.warn("결과를 받았으나 해당 주문이 없음: orderId={}", event.orderId()));
    }
}

package com.example.msa.product;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 주문 생성 이벤트를 받아 재고 확보를 맡기고, <b>그 결과를 반드시 되돌려 보낸다</b>.
 *
 * <p>Phase 4 에서는 재고가 부족하면 로그만 남기고 이벤트를 버렸다. 그 결과 주문은
 * "성공"으로 남고 재고는 그대로인, 서로 어긋난 상태가 되었다. 서비스마다 DB 가
 * 분리되어 있으므로 하나의 트랜잭션으로 묶어 함께 롤백할 수 없기 때문이다.
 *
 * <p>대신 <b>결과를 알리고, 듣는 쪽이 스스로 되돌리게</b> 한다. 이것이 Saga 다.
 * 여기서 이 서비스가 할 일은 성공이든 실패든 결과를 발행하는 것까지다.
 * 주문을 어떻게 처리할지는 order-service 가 정한다.
 */
@Component
class OrderCreatedListener {

    private final StockReservationService reservationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    OrderCreatedListener(StockReservationService reservationService,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.reservationService = reservationService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = OrderCreatedEvent.TOPIC, groupId = "product-service")
    void handle(OrderCreatedEvent event) {
        StockResultEvent result = reservationService.reserve(event);

        // 발행은 트랜잭션 밖에서 한다. 브로커 전송이 느리거나 실패할 때
        // DB 커넥션과 락을 붙잡고 있지 않기 위해서다.
        //
        // ponytail: 그래서 DB 커밋과 발행이 여전히 원자적이지 않다(dual write).
        // 다만 처리 기록을 남겨 두었으므로, 발행 직전에 죽더라도 재전송 때
        // 같은 결론이 다시 나간다. 완전한 해결은 Transactional Outbox 패턴이다.
        kafkaTemplate.send(StockResultEvent.TOPIC, result);
    }
}

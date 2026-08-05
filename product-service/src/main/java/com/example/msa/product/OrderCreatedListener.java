package com.example.msa.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 주문 생성 이벤트를 듣고 재고를 차감한 뒤, <b>그 결과를 반드시 되돌려 보낸다</b>.
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

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final ProductRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    OrderCreatedListener(ProductRepository repository, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    // ponytail: 같은 이벤트가 두 번 오면 재고가 두 번 깎인다. Kafka 는 at-least-once 라
    // 재전송이 일어날 수 있다. 실제로 다루려면 처리한 orderId 를 기록해 두고 건너뛰어야
    // 한다(멱등 소비). 주문 상태 변경 쪽은 같은 값을 두 번 써도 결과가 같아 문제없다.
    @KafkaListener(topics = OrderCreatedEvent.TOPIC, groupId = "product-service")
    void handle(OrderCreatedEvent event) {
        StockResultEvent result = repository.findById(event.productId())
                .map(product -> reserve(product, event))
                .orElseGet(() -> {
                    log.warn("존재하지 않는 상품에 대한 주문 이벤트: productId={}", event.productId());
                    return StockResultEvent.rejected(event.orderId(), event.productId(),
                            event.quantity(), "존재하지 않는 상품입니다");
                });

        // ponytail: DB 저장과 이벤트 발행이 한 트랜잭션이 아니다(dual write).
        // 저장 직후 이 서비스가 죽으면 재고는 깎였는데 결과가 발행되지 않아
        // 주문이 PENDING 으로 남는다. 실무에서는 Transactional Outbox 패턴으로
        // 이벤트를 같은 DB 트랜잭션에 기록한 뒤 별도 프로세스가 발행한다.
        kafkaTemplate.send(StockResultEvent.TOPIC, result);
    }

    private StockResultEvent reserve(Product product, OrderCreatedEvent event) {
        if (!product.decreaseStock(event.quantity())) {
            log.warn("재고 부족: productId={}, 주문수량={}, 현재재고={} (orderId={})",
                    event.productId(), event.quantity(), product.getStock(), event.orderId());
            return StockResultEvent.rejected(event.orderId(), event.productId(), event.quantity(),
                    "재고 부족 (요청 %d, 남은 재고 %d)".formatted(event.quantity(), product.getStock()));
        }

        repository.save(product);
        log.info("재고 차감: productId={}, 주문수량={}, 남은재고={} (orderId={})",
                event.productId(), event.quantity(), product.getStock(), event.orderId());
        return StockResultEvent.reserved(event.orderId(), event.productId(), event.quantity());
    }
}

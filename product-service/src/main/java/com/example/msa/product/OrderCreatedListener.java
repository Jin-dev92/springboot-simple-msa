package com.example.msa.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 주문 생성 이벤트를 듣고 재고를 차감한다.
 *
 * <p>order-service 는 이 클래스의 존재를 모른다. 토픽에 이벤트를 던졌을 뿐이고,
 * 그것을 누가 어떻게 처리하는지는 관여하지 않는다. 이 방향성 덕분에 이 서비스가
 * 잠시 멈춰 있어도 주문 자체는 계속 성공하고, 다시 뜨면 밀린 이벤트를 이어서 처리한다.
 */
@Component
class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final ProductRepository repository;

    OrderCreatedListener(ProductRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = OrderCreatedEvent.TOPIC, groupId = "product-service")
    void handle(OrderCreatedEvent event) {
        repository.findById(event.productId()).ifPresentOrElse(product -> {
            if (product.decreaseStock(event.quantity())) {
                repository.save(product);
                log.info("재고 차감: productId={}, 주문수량={}, 남은재고={} (orderId={})",
                        event.productId(), event.quantity(), product.getStock(), event.orderId());
            } else {
                // ponytail: 재고 부족 이벤트는 로그만 남기고 버린다. 예외를 던지면 Kafka 가
                // 무한 재시도하면서 뒤의 정상 이벤트까지 막는다(poison message).
                // 실제로 다뤄야 한다면 DLQ(Dead Letter Queue)와 보상 트랜잭션이 필요하다.
                log.warn("재고 부족으로 차감하지 않음: productId={}, 주문수량={}, 현재재고={}",
                        event.productId(), event.quantity(), product.getStock());
            }
        }, () -> log.warn("존재하지 않는 상품에 대한 주문 이벤트: productId={}", event.productId()));
    }
}

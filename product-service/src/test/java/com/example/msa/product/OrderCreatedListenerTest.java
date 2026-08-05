package com.example.msa.product;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 브로커 없이 리스너 메서드를 직접 호출해 재고 차감 규칙만 검증한다.
 * Kafka 가 메시지를 잘 나르는지는 스프링이 보장하는 부분이므로 여기서 다시 확인하지 않는다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class OrderCreatedListenerTest {

    @Autowired
    private OrderCreatedListener listener;

    @Autowired
    private ProductRepository repository;

    @Test
    void 주문_수량만큼_재고를_차감한다() {
        int before = repository.findById(1L).orElseThrow().getStock();

        listener.handle(new OrderCreatedEvent(1L, 1L, 3));

        assertThat(repository.findById(1L).orElseThrow().getStock()).isEqualTo(before - 3);
    }

    @Test
    void 재고보다_많이_주문하면_차감하지_않는다() {
        int before = repository.findById(2L).orElseThrow().getStock();

        listener.handle(new OrderCreatedEvent(2L, 2L, before + 1));

        assertThat(repository.findById(2L).orElseThrow().getStock()).isEqualTo(before);
    }

    @Test
    void 없는_상품_이벤트는_무시한다() {
        listener.handle(new OrderCreatedEvent(3L, 9999L, 1));
        // 예외를 던지면 Kafka 가 무한 재시도하므로, 조용히 넘어가야 한다.
    }
}

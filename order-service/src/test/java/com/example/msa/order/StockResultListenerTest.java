package com.example.msa.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Saga 의 마지막 구간만 떼어 검증한다. 브로커 없이 리스너 메서드를 직접 호출한다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class StockResultListenerTest {

    @Autowired
    private StockResultListener listener;

    @Autowired
    private OrderRepository repository;

    private Order pendingOrder() {
        return repository.save(new Order(1L, 1L, 3, new BigDecimal("267000")));
    }

    @Test
    void 주문은_생성_직후_PENDING_이다() {
        assertThat(pendingOrder().getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void 재고를_잡았다는_결과를_받으면_확정된다() {
        Order order = pendingOrder();

        listener.handle(new StockResultEvent(order.getId(), 1L, 3, true, null));

        Order updated = repository.findById(order.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(updated.getCancelReason()).isNull();
    }

    @Test
    void 재고를_못_잡았다는_결과를_받으면_취소된다() {
        Order order = pendingOrder();

        listener.handle(new StockResultEvent(order.getId(), 1L, 3, false, "재고 부족 (요청 3, 남은 재고 1)"));

        Order updated = repository.findById(order.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        // 취소된 주문도 삭제하지 않고 사유와 함께 남긴다.
        assertThat(updated.getCancelReason()).contains("재고 부족");
    }

    @Test
    void 같은_결과를_두_번_받아도_상태는_같다() {
        Order order = pendingOrder();
        StockResultEvent event = new StockResultEvent(order.getId(), 1L, 3, true, null);

        listener.handle(event);
        listener.handle(event);   // Kafka 는 at-least-once 라 재전송이 있을 수 있다

        assertThat(repository.findById(order.getId()).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void 없는_주문에_대한_결과는_무시한다() {
        listener.handle(new StockResultEvent(9999L, 1L, 1, true, null));
        // 예외를 던지면 Kafka 가 무한 재시도하므로 조용히 넘어가야 한다.
    }
}

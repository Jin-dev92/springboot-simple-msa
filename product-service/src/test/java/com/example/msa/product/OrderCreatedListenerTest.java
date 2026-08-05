package com.example.msa.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 브로커 없이 리스너 메서드를 직접 호출해 재고 차감 규칙과
 * <b>결과 이벤트 발행</b>을 검증한다. Saga 는 결과를 되돌려 보내야 성립하므로,
 * 실패 경로에서도 이벤트가 나가는지가 핵심이다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class OrderCreatedListenerTest {

    @Autowired
    private OrderCreatedListener listener;

    @Autowired
    private ProductRepository repository;

    @MockitoBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    private StockResultEvent published() {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(StockResultEvent.TOPIC), captor.capture());
        return (StockResultEvent) captor.getValue();
    }

    @Test
    void 주문_수량만큼_재고를_차감하고_성공을_알린다() {
        int before = repository.findById(1L).orElseThrow().getStock();

        listener.handle(new OrderCreatedEvent(1L, 1L, 3));

        assertThat(repository.findById(1L).orElseThrow().getStock()).isEqualTo(before - 3);
        assertThat(published().reserved()).isTrue();
    }

    @Test
    void 재고보다_많이_주문하면_차감하지_않고_실패를_알린다() {
        int before = repository.findById(2L).orElseThrow().getStock();

        listener.handle(new OrderCreatedEvent(2L, 2L, before + 1));

        assertThat(repository.findById(2L).orElseThrow().getStock()).isEqualTo(before);

        StockResultEvent result = published();
        assertThat(result.reserved()).isFalse();
        // 사유를 담아야 order-service 가 사용자에게 왜 취소됐는지 알려줄 수 있다.
        assertThat(result.reason()).contains("재고 부족");
    }

    @Test
    void 없는_상품_이벤트도_실패로_알린다() {
        listener.handle(new OrderCreatedEvent(3L, 9999L, 1));

        StockResultEvent result = published();
        assertThat(result.reserved()).isFalse();
        // 조용히 버리면 주문이 PENDING 으로 영원히 남는다. 반드시 결과를 보내야 한다.
        assertThat(result.orderId()).isEqualTo(3L);
    }
}

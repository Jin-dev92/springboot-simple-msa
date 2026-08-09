package com.example.msa.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 브로커 없이 리스너를 직접 호출해 재고 명령 처리와 응답 발행을 검증한다.
 * 실행(RESERVE)과 보상(RELEASE)이 서로를 막지 않는지가 이 전환의 핵심이다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class StockCommandListenerTest {

    @Autowired
    private StockCommandListener listener;

    @Autowired
    private ProductRepository repository;

    @MockitoBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    private SagaReply published() {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(SagaReply.TOPIC), any(String.class), captor.capture());
        return (SagaReply) captor.getValue();
    }

    private static StockCommand reserve(long orderId, long productId, int quantity) {
        return new StockCommand(orderId, productId, quantity, StockCommand.Action.RESERVE);
    }

    private static StockCommand release(long orderId, long productId, int quantity) {
        return new StockCommand(orderId, productId, quantity, StockCommand.Action.RELEASE);
    }

    private int stockOf(long productId) {
        return repository.findById(productId).orElseThrow().getStock();
    }

    @Test
    void 주문_수량만큼_재고를_차감하고_성공을_알린다() {
        int before = stockOf(1L);

        listener.handle(reserve(1L, 1L, 3));

        assertThat(stockOf(1L)).isEqualTo(before - 3);

        SagaReply reply = published();
        assertThat(reply.success()).isTrue();
        assertThat(reply.action()).isEqualTo("RESERVE");
    }

    @Test
    void 재고보다_많이_주문하면_차감하지_않고_실패를_알린다() {
        int before = stockOf(2L);

        listener.handle(reserve(2L, 2L, before + 1));

        assertThat(stockOf(2L)).isEqualTo(before);

        SagaReply reply = published();
        assertThat(reply.success()).isFalse();
        assertThat(reply.reason()).contains("재고 부족");
    }

    @Test
    void 없는_상품_명령도_실패로_알린다() {
        listener.handle(reserve(3L, 9999L, 1));

        SagaReply reply = published();
        assertThat(reply.success()).isFalse();
        // 조용히 버리면 Saga 가 영원히 대기 상태로 남는다.
        assertThat(reply.orderId()).isEqualTo(3L);
    }

    @Test
    void 같은_명령이_두_번_와도_재고는_한_번만_차감된다() {
        int before = stockOf(1L);
        StockCommand command = reserve(10L, 1L, 4);

        listener.handle(command);
        listener.handle(command);   // Kafka 는 at-least-once 라 재전송이 있을 수 있다

        assertThat(stockOf(1L)).isEqualTo(before - 4);
    }

    @Test
    void 중복_명령에도_같은_결과를_다시_발행한다() {
        StockCommand command = reserve(11L, 1L, 2);

        listener.handle(command);
        listener.handle(command);

        // 두 번 발행되어야 한다. 첫 처리에서 발행이 실패했을 수 있으므로
        // 조용히 건너뛰면 Saga 가 대기 상태로 남는다.
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, times(2))
                .send(eq(SagaReply.TOPIC), any(String.class), captor.capture());

        assertThat(captor.getAllValues())
                .allSatisfy(v -> assertThat(((SagaReply) v).success()).isTrue());
    }

    @Test
    void 보상_명령은_재고를_되돌리고_성공을_알린다() {
        int before = stockOf(3L);
        listener.handle(reserve(20L, 3L, 5));
        assertThat(stockOf(3L)).isEqualTo(before - 5);

        listener.handle(release(20L, 3L, 5));

        assertThat(stockOf(3L)).isEqualTo(before);
    }

    @Test
    void 같은_주문의_보상은_차감_기록에_막히지_않는다() {
        // 멱등 키가 orderId 단독이면 이 테스트가 깨진다. (orderId, action) 이어야 한다.
        int before = stockOf(1L);
        listener.handle(reserve(21L, 1L, 6));

        listener.handle(release(21L, 1L, 6));

        assertThat(stockOf(1L)).isEqualTo(before);
    }

    @Test
    void 보상_명령이_두_번_와도_재고는_한_번만_복구된다() {
        int before = stockOf(2L);
        listener.handle(reserve(22L, 2L, 3));
        StockCommand compensation = release(22L, 2L, 3);

        listener.handle(compensation);
        listener.handle(compensation);

        // 두 번 복구되면 재고가 원래보다 많아진다. 보상도 반드시 멱등해야 한다.
        assertThat(stockOf(2L)).isEqualTo(before);
    }
}

package com.example.msa.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 브로커 없이 오케스트레이터를 직접 호출해 <b>상태 전이와 다음 명령</b>을 검증한다.
 *
 * <p>코레오그래피에서는 이 검증을 한 곳에서 할 수 없었다. 흐름이 두 서비스의
 * 리스너에 나뉘어 있었기 때문이다. 흐름이 한 클래스에 모인 덕에 통합 환경 없이
 * 전체 시나리오를 돌려볼 수 있게 되었다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        // Task 7 에서 타임아웃 스위퍼가 들어오면 10초마다 돌면서 이 테스트가 만든
        // 사가를 건드릴 수 있다. 주기를 길게 잡아 간섭을 막는다.
        "saga.timeout.check-interval=1h"
})
class OrderSagaOrchestratorTest {

    @Autowired
    private OrderSagaOrchestrator orchestrator;

    @Autowired
    private OrderRepository orders;

    @Autowired
    private OrderSagaRepository sagas;

    @MockitoBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    /** 주문을 만들고 Saga 를 시작해, 재고 응답을 기다리는 상태로 만든다. */
    private Order startedOrder() {
        Order order = orders.save(new Order(1L, 2L, "모니터", 4, new BigDecimal("1280000")));
        orchestrator.start(order);
        return order;
    }

    private static SagaReply ok(Long orderId, String action) {
        return new SagaReply(orderId, action, true, null);
    }

    private static SagaReply fail(Long orderId, String action, String reason) {
        return new SagaReply(orderId, action, false, reason);
    }

    private Object lastSentTo(String topic) {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(topic), any(String.class), captor.capture());
        return captor.getValue();
    }

    private SagaStep stepOf(Long orderId) {
        return sagas.findById(orderId).orElseThrow().getStep();
    }

    private Order reloaded(Long orderId) {
        return orders.findById(orderId).orElseThrow();
    }

    @Test
    void 주문이_시작되면_재고_확보_명령을_보낸다() {
        Order order = startedOrder();

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.RESERVING_STOCK);

        StockCommand command = (StockCommand) lastSentTo(StockCommand.TOPIC);
        assertThat(command.action()).isEqualTo(StockCommand.Action.RESERVE);
        assertThat(command.productId()).isEqualTo(2L);
        assertThat(command.quantity()).isEqualTo(4);
    }

    @Test
    void 재고를_확보하면_결제_명령을_보낸다() {
        Order order = startedOrder();

        orchestrator.onReply(ok(order.getId(), "RESERVE"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.CHARGING_PAYMENT);

        PaymentCommand command = (PaymentCommand) lastSentTo(PaymentCommand.TOPIC);
        assertThat(command.action()).isEqualTo(PaymentCommand.Action.CHARGE);
        assertThat(command.userId()).isEqualTo(1L);
        assertThat(command.amount()).isEqualByComparingTo("1280000");
    }

    @Test
    void 결제까지_성공하면_주문을_확정한다() {
        Order order = startedOrder();
        orchestrator.onReply(ok(order.getId(), "RESERVE"));

        orchestrator.onReply(ok(order.getId(), "CHARGE"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.COMPLETED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }

    @Test
    void 재고_확보에_실패하면_되돌릴_것_없이_바로_취소한다() {
        Order order = startedOrder();

        orchestrator.onReply(fail(order.getId(), "RESERVE", "재고 부족 (요청 4, 남은 재고 1)"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.FAILED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(reloaded(order.getId()).getCancelReason()).contains("재고 부족");

        // 앞 단계가 없으므로 보상 명령이 나가면 안 된다.
        verify(kafkaTemplate, never())
                .send(eq(PaymentCommand.TOPIC), any(String.class), any());
    }

    @Test
    void 결제에_실패하면_재고를_되돌리는_보상_명령을_보낸다() {
        Order order = startedOrder();
        orchestrator.onReply(ok(order.getId(), "RESERVE"));

        orchestrator.onReply(fail(order.getId(), "CHARGE", "잔액 부족 (청구 1280000, 잔액 1000000)"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.COMPENSATING_STOCK);
        // 아직 주문을 취소하지 않는다. 보상이 끝났는지 확인한 뒤에 최종 상태로 간다.
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.PENDING);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, org.mockito.Mockito.times(2))
                .send(eq(StockCommand.TOPIC), any(String.class), captor.capture());

        StockCommand compensation = (StockCommand) captor.getAllValues().get(1);
        assertThat(compensation.action()).isEqualTo(StockCommand.Action.RELEASE);
        assertThat(compensation.quantity()).isEqualTo(4);
    }

    @Test
    void 보상이_끝나면_결제_실패_사유로_주문을_취소한다() {
        Order order = startedOrder();
        orchestrator.onReply(ok(order.getId(), "RESERVE"));
        orchestrator.onReply(fail(order.getId(), "CHARGE", "잔액 부족 (청구 1280000, 잔액 1000000)"));

        orchestrator.onReply(ok(order.getId(), "RELEASE"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.FAILED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CANCELLED);
        // 사용자에게는 "재고를 되돌렸다"가 아니라 왜 실패했는지를 알려야 한다.
        assertThat(reloaded(order.getId()).getCancelReason()).contains("잔액 부족");
    }

    @Test
    void 기다리는_단계와_어긋난_응답은_무시한다() {
        Order order = startedOrder();

        // RESERVE 를 기다리는데 CHARGE 응답이 왔다. 타임아웃 뒤 늦게 도착한 경우다.
        orchestrator.onReply(ok(order.getId(), "CHARGE"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.RESERVING_STOCK);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void 이미_끝난_사가에_늦게_도착한_응답은_상태를_되살리지_못한다() {
        Order order = startedOrder();
        orchestrator.onReply(fail(order.getId(), "RESERVE", "재고 부족"));

        // 취소된 뒤에 성공 응답이 뒤늦게 도착했다.
        orchestrator.onReply(ok(order.getId(), "RESERVE"));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.FAILED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void 모르는_주문의_응답은_조용히_버린다() {
        // sagas.findById(999999L) 가 비어 있다는 것은 onReply 가 무엇을 하든 참이라
        // 의미가 없다. 실제로 지켜야 할 것은 "던지지 않는다"와 "아무것도 발행하지
        // 않는다"이다.
        assertThatNoException().isThrownBy(() -> orchestrator.onReply(ok(999999L, "RESERVE")));

        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}

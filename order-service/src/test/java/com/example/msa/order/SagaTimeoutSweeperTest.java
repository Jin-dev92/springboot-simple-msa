package com.example.msa.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 응답이 오지 않은 채 시간이 지난 Saga 를 스위퍼가 걷어내는지 확인한다.
 *
 * <p>코레오그래피에서는 이 테스트를 쓸 수조차 없었다. "어디서 멈췄는가"를 아무도
 * 들고 있지 않았기 때문이다. 진행 상태를 한 곳에 모은 덕에 가능해진 기능이다.
 *
 * <p>시간을 기다리는 대신 {@code updated_at} 을 과거로 직접 밀어 넣는다. 테스트가
 * 30초를 실제로 기다릴 이유가 없다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        // 스케줄러가 테스트 도중 제멋대로 돌지 않도록 주기를 아주 길게 잡는다.
        // 검증은 sweep() 을 직접 불러서 한다.
        "saga.timeout.check-interval=1h",
        // 릴레이도 마찬가지다. 브로커가 없는데 발행을 시도하면 로그만 시끄러워진다.
        "outbox.poll-interval=1h"
})
class SagaTimeoutSweeperTest {

    @Autowired
    private SagaTimeoutSweeper sweeper;

    @Autowired
    private OrderSagaOrchestrator orchestrator;

    @Autowired
    private OrderRepository orders;

    @Autowired
    private OrderSagaRepository sagas;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private ObjectMapper objectMapper;

    /** outbox 에 쌓인 해당 토픽의 메시지를 삽입 순서대로 돌려준다. */
    private <T> List<T> outboxed(String topic, Class<T> type) {
        return outbox.findAll(Sort.by("id")).stream()
                .filter(m -> m.getTopic().equals(topic))
                .map(m -> {
                    try {
                        return objectMapper.readValue(m.getPayload(), type);
                    } catch (Exception e) {
                        throw new IllegalStateException("outbox payload 역직렬화 실패", e);
                    }
                })
                .toList();
    }

    /**
     * OrderSagaOrchestratorTest 와 같은 컨텍스트·H2(orderdb)를 공유한다. 그 쪽이
     * CHARGING_PAYMENT, COMPENSATING_STOCK 에 사가를 남겨 둔 채 끝나므로, 두 클래스
     * 실행 사이에 30초(sweep 임계) 이상 벌어지면 sweep() 이 그 잔여물까지 함께
     * 걷어가 발행 횟수(times(1/2/3)) 단언이 깨진다. 매 테스트 전에 비워 격리한다.
     */
    @BeforeEach
    void 남은_사가를_비운다() {
        sagas.deleteAll();
        orders.deleteAll();
        outbox.deleteAll();
    }

    private Order startedOrder() {
        Order order = orders.save(new Order(1L, 2L, "모니터", 4, new BigDecimal("1280000")));
        orchestrator.start(order);
        return order;
    }

    /**
     * 마지막 갱신 시각을 과거로 민다. 응답 없이 오래 머문 상태를 흉내 낸다.
     *
     * <p>테이블·컬럼 이름은 JPA 기본 명명 규칙(카멜케이스 → 스네이크케이스)을 따른
     * {@code order_saga.updated_at} 이다. {@code Instant} 를 그대로 바인딩하는데,
     * H2 드라이버가 이를 거부하면 {@code java.sql.Timestamp.from(...)} 으로 감싼다.
     */
    private void ageBy(Long orderId, int seconds) {
        jdbc.update("update order_saga set updated_at = ? where order_id = ?",
                Instant.now().minusSeconds(seconds), orderId);
    }

    private SagaStep stepOf(Long orderId) {
        return sagas.findById(orderId).orElseThrow().getStep();
    }

    private Order reloaded(Long orderId) {
        return orders.findById(orderId).orElseThrow();
    }

    @Test
    void 아직_임계를_넘지_않은_사가는_건드리지_않는다() {
        Order order = startedOrder();
        ageBy(order.getId(), 5);

        sweeper.sweep();

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.RESERVING_STOCK);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void 재고_응답이_없으면_되돌릴_것_없이_취소한다() {
        Order order = startedOrder();
        ageBy(order.getId(), 60);

        sweeper.sweep();

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.FAILED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(reloaded(order.getId()).getCancelReason()).contains("재고 확보 응답이 없어");

        // 잡은 재고가 없으므로 보상 명령이 나가면 안 된다. (start 의 RESERVE 1건뿐)
        List<StockCommand> commands = outboxed(StockCommand.TOPIC, StockCommand.class);
        assertThat(commands).hasSize(1);
        assertThat(commands.get(0).action()).isEqualTo(StockCommand.Action.RESERVE);
    }

    @Test
    void 결제_응답이_없으면_잡은_재고를_되돌린다() {
        Order order = startedOrder();
        orchestrator.onReply(new SagaReply(order.getId(), "RESERVE", true, null));
        ageBy(order.getId(), 60);

        sweeper.sweep();

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.COMPENSATING_STOCK);

        List<StockCommand> commands = outboxed(StockCommand.TOPIC, StockCommand.class);
        assertThat(commands).hasSize(2);
        assertThat(commands.get(1).action()).isEqualTo(StockCommand.Action.RELEASE);
    }

    @Test
    void 보상_응답이_없으면_보상_명령을_다시_보낸다() {
        Order order = startedOrder();
        orchestrator.onReply(new SagaReply(order.getId(), "RESERVE", true, null));
        orchestrator.onReply(new SagaReply(order.getId(), "CHARGE", false, "잔액 부족"));
        ageBy(order.getId(), 60);

        sweeper.sweep();

        // 단계는 그대로 두고 명령만 재발행한다. 재고는 반드시 되돌아가야 하므로
        // 횟수 상한을 두지 않는다. 참여자 쪽이 멱등하므로 여러 번 보내도 안전하다.
        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.COMPENSATING_STOCK);
        assertThat(outboxed(StockCommand.TOPIC, StockCommand.class)).hasSize(3);
    }

    @Test
    void 이미_끝난_사가는_아무리_오래돼도_건드리지_않는다() {
        Order order = startedOrder();
        orchestrator.onReply(new SagaReply(order.getId(), "RESERVE", true, null));
        orchestrator.onReply(new SagaReply(order.getId(), "CHARGE", true, null));
        ageBy(order.getId(), 3600);

        sweeper.sweep();

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.COMPLETED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        assertThat(outboxed(StockCommand.TOPIC, StockCommand.class))
                .noneMatch(c -> c.action() == StockCommand.Action.RELEASE);
    }

    @Test
    void 타임아웃_뒤_늦게_도착한_성공_응답은_취소를_되살리지_못한다() {
        Order order = startedOrder();
        ageBy(order.getId(), 60);
        sweeper.sweep();

        // payment-service 가 되살아나 뒤늦게 성공을 답했다.
        orchestrator.onReply(new SagaReply(order.getId(), "RESERVE", true, null));

        assertThat(stepOf(order.getId())).isEqualTo(SagaStep.FAILED);
        assertThat(reloaded(order.getId()).getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }
}

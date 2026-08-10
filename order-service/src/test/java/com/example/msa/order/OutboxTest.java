package com.example.msa.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;

/**
 * Phase 10 의 검증 기준. <b>브로커가 없어도 메시지가 유실되지 않는가.</b>
 *
 * <p>이전 구조에서는 커밋 직후 {@code kafkaTemplate.send} 를 호출했다. 브로커가
 * 죽어 있으면 그 호출이 실패하고, 명령은 영영 나가지 않은 채 사가만 대기 상태로
 * 남았다. 스위퍼가 30초 뒤 걷어내긴 하지만 <b>주문은 취소</b>된다.
 *
 * <p>Outbox 를 거치면 메시지가 DB 에 남으므로, 브로커가 돌아왔을 때 릴레이가 마저
 * 내보낸다. 주문이 살아남는다. 이 테스트는 그 전반부 — <b>브로커 없이도 메시지가
 * DB 에 안전하게 남는가</b> — 를 확인한다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "saga.timeout.check-interval=1h",
        // 릴레이를 수동으로만 돌린다. 자동으로 돌면 브로커가 없어 실패할 뿐이다.
        "outbox.poll-interval=1h",
        // 브로커가 없으므로 기동 시 재구축을 끈다.
        "replica.rebuild-on-startup=false",
        "replica.verify.interval=1h"
})
class OutboxTest {

    @Autowired
    private OrderSagaOrchestrator orchestrator;

    @Autowired
    private OrderRepository orders;

    @Autowired
    private OrderSagaRepository sagas;

    @Autowired
    private OutboxRepository outbox;

    @Autowired
    private OutboxRelay relay;

    @BeforeEach
    void 비운다() {
        outbox.deleteAll();
        sagas.deleteAll();
        orders.deleteAll();
    }

    private Order startedOrder() {
        Order order = orders.save(new Order(1L, 2L, "모니터", 4, new BigDecimal("1280000")));
        orchestrator.start(order);
        return order;
    }

    @Test
    void 브로커가_없어도_주문과_메시지가_함께_남는다() {
        Order order = startedOrder();

        // 주문도, 사가도, 메시지도 전부 커밋되어 있다.
        assertThat(orders.findById(order.getId())).isPresent();
        assertThat(sagas.findById(order.getId())).isPresent();
        assertThat(outbox.findAll()).singleElement().satisfies(m -> {
            assertThat(m.getTopic()).isEqualTo(StockCommand.TOPIC);
            assertThat(m.getMessageKey()).isEqualTo(String.valueOf(order.getId()));
            assertThat(m.getPublishedAt()).isNull();
        });
    }

    @Test
    void 발행_전까지는_미발행_상태로_남아_릴레이가_다시_집어_간다() {
        startedOrder();

        // 브로커가 없으므로 릴레이가 돌아도 발행에 실패한다.
        relay.publishPending();

        // 메시지가 사라지지 않고 그대로 남아 있어야 한다. 이것이 유실 방지의 핵심이다.
        assertThat(outbox.findAll()).singleElement()
                .satisfies(m -> assertThat(m.getPublishedAt())
                        .as("발행에 실패했으므로 미발행으로 남아야 한다").isNull());
    }

    @Test
    void 메시지는_삽입_순서대로_쌓인다() {
        Order order = startedOrder();
        orchestrator.onReply(new SagaReply(order.getId(), "RESERVE", true, null));
        orchestrator.onReply(new SagaReply(order.getId(), "CHARGE", false, "잔액 부족"));

        // RESERVE → CHARGE → RELEASE 순서가 id 오름차순으로 보존되어야 한다.
        // 릴레이가 이 순서로 내보내므로, 여기가 뒤바뀌면 재고가 어긋난다.
        assertThat(outbox.findAll(Sort.by("id")))
                .extracting(OutboxMessage::getTopic)
                .containsExactly(StockCommand.TOPIC, PaymentCommand.TOPIC, StockCommand.TOPIC);
    }

    @Test
    void 발행에_성공하면_발행_시각이_찍혀_다시_나가지_않는다() {
        startedOrder();
        OutboxMessage message = outbox.findAll().get(0);

        // 브로커가 없어 실제 발행은 못 하므로, 발행 성공을 직접 표시해 본다.
        message.markPublished();
        outbox.save(message);

        relay.publishPending();

        // 이미 발행된 것은 릴레이의 조회 대상에서 빠져 중복 발행되지 않는다.
        assertThat(outbox.findTop100ByPublishedAtIsNullOrderByIdAsc()).isEmpty();
    }
}

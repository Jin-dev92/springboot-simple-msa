package com.example.msa.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 브로커 없이 리스너를 직접 호출해 잔액 차감 규칙과 응답 발행을 검증한다.
 * Saga 는 결과를 되돌려 보내야 성립하므로, 실패 경로에서도 응답이 나가는지가 핵심이다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class PaymentCommandListenerTest {

    @Autowired
    private PaymentCommandListener listener;

    @Autowired
    private AccountRepository accounts;

    @MockitoBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    private SagaReply published() {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(SagaReply.TOPIC), any(String.class), captor.capture());
        return (SagaReply) captor.getValue();
    }

    private static PaymentCommand charge(long orderId, long userId, String amount) {
        return new PaymentCommand(orderId, userId, new BigDecimal(amount),
                PaymentCommand.Action.CHARGE);
    }

    @Test
    void 잔액이_충분하면_차감하고_성공을_알린다() {
        BigDecimal before = accounts.findById(1L).orElseThrow().getBalance();

        listener.handle(charge(1L, 1L, "89000"));

        assertThat(accounts.findById(1L).orElseThrow().getBalance())
                .isEqualByComparingTo(before.subtract(new BigDecimal("89000")));

        SagaReply reply = published();
        assertThat(reply.success()).isTrue();
        assertThat(reply.action()).isEqualTo("CHARGE");
        assertThat(reply.orderId()).isEqualTo(1L);
    }

    @Test
    void 잔액을_넘으면_차감하지_않고_실패를_알린다() {
        BigDecimal before = accounts.findById(2L).orElseThrow().getBalance();

        listener.handle(charge(2L, 2L, "1280000"));

        assertThat(accounts.findById(2L).orElseThrow().getBalance()).isEqualByComparingTo(before);

        SagaReply reply = published();
        assertThat(reply.success()).isFalse();
        // 사유를 담아야 order-service 가 사용자에게 왜 취소됐는지 알려줄 수 있다.
        assertThat(reply.reason()).contains("잔액 부족");
    }

    @Test
    void 계좌가_없으면_실패로_알린다() {
        listener.handle(charge(3L, 9999L, "1000"));

        SagaReply reply = published();
        assertThat(reply.success()).isFalse();
        // 조용히 버리면 Saga 가 영원히 대기 상태로 남는다. 반드시 응답을 보내야 한다.
        assertThat(reply.orderId()).isEqualTo(3L);
    }

    @Test
    void 같은_명령이_두_번_와도_잔액은_한_번만_차감된다() {
        BigDecimal before = accounts.findById(1L).orElseThrow().getBalance();
        PaymentCommand command = charge(10L, 1L, "50000");

        listener.handle(command);
        listener.handle(command);   // Kafka 는 at-least-once 라 재전송이 있을 수 있다

        assertThat(accounts.findById(1L).orElseThrow().getBalance())
                .isEqualByComparingTo(before.subtract(new BigDecimal("50000")));
    }

    @Test
    void 중복_명령에도_같은_결과를_다시_발행한다() {
        PaymentCommand command = charge(11L, 1L, "1000");

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
}

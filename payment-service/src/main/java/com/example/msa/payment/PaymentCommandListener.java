package com.example.msa.payment;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 결제 명령을 받아 처리하고 <b>결과를 반드시 되돌려 보낸다</b>.
 *
 * <p>이 클래스에는 흐름 판단이 없다. 성공하면 다음에 무엇을 할지, 실패하면 무엇을
 * 되돌릴지는 전부 오케스트레이터가 정한다. 참여자는 시키는 일을 하고 답할 뿐이다.
 * 그래서 Saga 의 단계 순서를 바꿔도 이 파일은 건드리지 않는다.
 */
@Component
class PaymentCommandListener {

    private final PaymentService paymentService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    PaymentCommandListener(PaymentService paymentService,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = PaymentCommand.TOPIC, groupId = "payment-service")
    void handle(PaymentCommand command) {
        SagaReply reply = paymentService.handle(command);

        // 발행은 트랜잭션 밖에서 한다. 브로커 전송이 느리거나 실패할 때
        // DB 커넥션과 락을 붙잡고 있지 않기 위해서다.
        //
        // 키를 orderId 로 지정한다. 같은 주문의 응답이 같은 파티션에 들어가야
        // 오케스트레이터가 받는 순서가 보낸 순서와 같아진다.
        //
        // ponytail: DB 커밋과 발행이 여전히 원자적이지 않다(dual write).
        // 처리 기록을 남겨 두었으므로 발행 직전에 죽더라도 재전송 때 같은 결론이
        // 다시 나가고, 그마저 실패하면 오케스트레이터의 타임아웃이 걷어낸다.
        // 완전한 해결은 Transactional Outbox 패턴이다.
        kafkaTemplate.send(SagaReply.TOPIC, String.valueOf(reply.orderId()), reply);
    }
}

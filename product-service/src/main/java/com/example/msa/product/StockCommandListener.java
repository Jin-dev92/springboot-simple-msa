package com.example.msa.product;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 재고 명령을 받아 처리하고 <b>결과를 반드시 되돌려 보낸다</b>. {@code OrderCreatedListener} 를 대체한다.
 *
 * <p>바뀐 것은 "무엇을 듣는가"다. 예전에는 주문 생성이라는 사실을 듣고 재고를 잡을지
 * 스스로 판단했다. 이제는 잡으라는 지시, 되돌리라는 지시를 듣는다. 이 서비스는 자기
 * 다음에 결제 단계가 있다는 사실을 모른다. 그래서 단계를 끼워 넣거나 순서를 바꿔도
 * 이 파일은 건드리지 않는다.
 */
@Component
class StockCommandListener {

    private final StockReservationService reservationService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    StockCommandListener(StockReservationService reservationService,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.reservationService = reservationService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = StockCommand.TOPIC, groupId = "product-service")
    void handle(StockCommand command) {
        SagaReply reply = reservationService.handle(command);

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

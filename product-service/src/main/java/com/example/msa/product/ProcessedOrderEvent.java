package com.example.msa.product;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * 이미 처리한 주문 이벤트의 기록.
 *
 * <p>Kafka 는 at-least-once 다. 컨슈머가 메시지를 처리하고 오프셋을 커밋하기 직전에
 * 죽거나, 리밸런싱이 일어나면 같은 메시지가 다시 전달된다. 재고 차감처럼
 * <b>누적되는 연산</b>은 그때마다 값이 달라지므로 반드시 걸러야 한다.
 *
 * <p>{@code orderId} 를 기본키로 삼는 것이 핵심이다. 자동 증가 id 를 따로 두고
 * orderId 를 일반 컬럼에 넣으면 중복 행이 들어갈 수 있다. 기본키로 두면 DB 가
 * 유일성을 보장한다.
 *
 * <p>결과({@code reserved}, {@code reason})까지 함께 남기는 이유는 재전송 때
 * <b>같은 결론을 다시 발행</b>하기 위해서다. 건너뛰기만 하면, 첫 처리에서 결과 발행이
 * 실패했을 경우 주문이 PENDING 으로 영원히 남는다.
 */
@Entity
@Table(name = "processed_order_events")
class ProcessedOrderEvent {

    @Id
    private Long orderId;

    private boolean reserved;

    private String reason;

    private Instant processedAt;

    protected ProcessedOrderEvent() {
        // JPA 기본 생성자
    }

    ProcessedOrderEvent(Long orderId, boolean reserved, String reason) {
        this.orderId = orderId;
        this.reserved = reserved;
        this.reason = reason;
        this.processedAt = Instant.now();
    }

    boolean isReserved() {
        return reserved;
    }

    String getReason() {
        return reason;
    }

    Instant getProcessedAt() {
        return processedAt;
    }
}

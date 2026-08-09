package com.example.msa.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.Instant;

/**
 * 이미 처리한 명령의 기록.
 *
 * <p>Kafka 는 at-least-once 다. 컨슈머가 처리하고 오프셋을 커밋하기 직전에 죽거나
 * 리밸런싱이 일어나면 같은 메시지가 다시 온다. 잔액 차감처럼 <b>누적되는 연산</b>은
 * 그때마다 값이 달라지므로 반드시 걸러야 한다.
 *
 * <p>키가 주문 번호 단독이 아니라 <b>주문 번호와 동작의 조합</b>인 것이 핵심이다.
 * 같은 주문에 CHARGE 와 (나중에 추가될) REFUND 가 모두 올 수 있는데, 키가 주문
 * 번호뿐이면 보상이 "이미 처리함"으로 무시되어 영영 되돌아가지 않는다.
 *
 * <p>복합키를 {@code @EmbeddedId} 대신 문자열 하나로 합쳐 쓴다. 복합키 클래스는
 * equals/hashCode/Serializable 을 요구하는데, 우리는 이 키로 부분 조회를 할 일이
 * 없어 그 보일러플레이트만큼의 값어치가 없다.
 * ponytail: 동작별 집계 같은 질의가 필요해지면 그때 @EmbeddedId 로 쪼갠다.
 */
@Entity
class ProcessedCommand {

    @Id
    private String id;

    private boolean success;

    private String reason;

    private Instant processedAt;

    protected ProcessedCommand() {
        // JPA 기본 생성자
    }

    ProcessedCommand(Long orderId, PaymentCommand.Action action, boolean success, String reason) {
        this.id = idOf(orderId, action);
        this.success = success;
        this.reason = reason;
        this.processedAt = Instant.now();
    }

    static String idOf(Long orderId, PaymentCommand.Action action) {
        return orderId + ":" + action;
    }

    boolean isSuccess() {
        return success;
    }

    String getReason() {
        return reason;
    }

    Instant getProcessedAt() {
        return processedAt;
    }
}

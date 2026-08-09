package com.example.msa.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import java.time.Instant;

/**
 * 주문 Saga 의 진행 상태.
 *
 * <p>코레오그래피에는 이 테이블에 해당하는 것이 없었다. 각 서비스가 이벤트를 듣고
 * 자기 일만 했을 뿐, <b>전체가 어디까지 왔는지</b>를 아무도 들고 있지 않았다.
 * 그래서 주문이 PENDING 에서 멈춰 있어도 어디서 멈췄는지 알 수 없었고,
 * "일정 시간 안에 끝나지 않으면 되돌린다"는 처리를 만들 수 없었다.
 *
 * <p>이 엔티티가 주문과 <b>같은 DB</b>에 있는 것이 오케스트레이터를 order-service 에
 * 둔 실익이다. 단계를 옮기는 쓰기와 주문 상태를 바꾸는 쓰기가 한 트랜잭션에 묶여,
 * 둘이 어긋난 상태가 생기지 않는다.
 *
 * <p>주문 하나에 Saga 하나이므로 {@code orderId} 를 그대로 기본키로 쓴다.
 */
@Entity
public class OrderSaga {

    @Id
    private Long orderId;

    @Enumerated(EnumType.STRING)
    private SagaStep step;

    /** 최종 실패 사유. 결제 실패처럼 보상 중에도 기억해 두었다가 주문 취소 사유로 옮긴다. */
    private String failReason;

    /** 타임아웃 판정 기준. 단계가 바뀌거나 명령을 재발행할 때마다 갱신한다. */
    private Instant updatedAt;

    /**
     * 동시 수정 감지: 스위퍼 스레드와 응답 리스너가 같은 사가를 동시에 옮기면
     * 나중 쓰기가 앞의 것을 덮어쓴다. 진 쪽 트랜잭션이 예외로 롤백되어 재처리된다.
     */
    @Version
    private long version;

    protected OrderSaga() {
        // JPA 기본 생성자
    }

    OrderSaga(Long orderId) {
        this.orderId = orderId;
        this.step = SagaStep.RESERVING_STOCK;
        this.updatedAt = Instant.now();
    }

    void moveTo(SagaStep step) {
        this.step = step;
        this.updatedAt = Instant.now();
    }

    void moveTo(SagaStep step, String failReason) {
        this.failReason = failReason;
        moveTo(step);
    }

    /** 단계는 그대로 두고 대기 시각만 미룬다. 같은 명령을 재발행할 때 쓴다. */
    void touch() {
        this.updatedAt = Instant.now();
    }

    public Long getOrderId() {
        return orderId;
    }

    public SagaStep getStep() {
        return step;
    }

    public String getFailReason() {
        return failReason;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}

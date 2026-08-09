package com.example.msa.payment;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.math.BigDecimal;

/**
 * 사용자 계좌. Saga 의 두 번째 참여자가 다루는 자원이다.
 *
 * <p>userId 를 그대로 기본키로 쓴다. 사용자당 계좌가 하나뿐인 학습 범위이므로
 * 별도 계좌번호를 두면 대응 관계만 늘어난다. userId 값은 auth-service 가 발급한
 * 토큰의 uid 클레임에서 온다. 서비스마다 DB 가 분리되어 있으므로 외래키는 없다.
 */
@Entity
public class Account {

    @Id
    private Long userId;

    // 금액은 부동소수점 오차가 없는 BigDecimal 로 다룬다.
    private BigDecimal balance;

    protected Account() {
        // JPA 기본 생성자
    }

    public Account(Long userId, BigDecimal balance) {
        this.userId = userId;
        this.balance = balance;
    }

    /**
     * 잔액을 차감한다. 모자라면 차감하지 않고 false 를 돌려준다.
     *
     * <p>재고의 {@code decreaseStock} 과 같은 모양이다. 실패를 예외가 아니라
     * 반환값으로 알리는 이유는, 잔액 부족이 <b>정상적인 업무 결과</b>이지
     * 시스템 오류가 아니기 때문이다. 이 결과는 그대로 Saga 응답에 실린다.
     *
     * @return 차감에 성공했으면 true
     */
    public boolean withdraw(BigDecimal amount) {
        if (balance.compareTo(amount) < 0) {
            return false;
        }
        balance = balance.subtract(amount);
        return true;
    }

    /**
     * 잔액을 되돌린다.
     *
     * <p>지금은 결제가 마지막 단계라 호출될 경로가 없다. 뒤에 배송 같은 단계가
     * 붙으면 그때의 보상(REFUND)이 이 메서드를 쓴다.
     *
     * <p>이 근거는 Phase 8 통합 검증에서 틀린 것으로 드러났다. 타임아웃 경로가
     * 정확히 그 경로를 만든다 — 학습 노트 11절 참고.
     */
    public void deposit(BigDecimal amount) {
        balance = balance.add(amount);
    }

    public Long getUserId() {
        return userId;
    }

    public BigDecimal getBalance() {
        return balance;
    }
}

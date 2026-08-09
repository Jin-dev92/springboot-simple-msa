package com.example.msa.order;

import java.math.BigDecimal;

/**
 * 결제 참여자에게 보내는 명령.
 *
 * <p>금액을 명령에 담아 보낸다. payment-service 는 상품 가격을 모르고, 주문 시점에
 * 확정된 금액으로 결제해야 하기 때문이다.
 */
record PaymentCommand(Long orderId, Long userId, BigDecimal amount, Action action) {

    static final String TOPIC = "payment-command";

    /**
     * 지금은 CHARGE 뿐이다. 결제가 마지막 단계라 그 뒤에 실패할 단계가 없어
     * 보상(REFUND)이 호출될 경로가 없다. 배송 같은 단계를 붙이는 시점에 추가한다.
     *
     * <p>이 근거는 Phase 8 통합 검증에서 틀린 것으로 드러났다. 타임아웃 경로가
     * 정확히 그 경로를 만든다 — 학습 노트 11절 참고.
     */
    enum Action {
        CHARGE
    }
}

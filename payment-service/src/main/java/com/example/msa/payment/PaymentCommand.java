package com.example.msa.payment;

import java.math.BigDecimal;

/**
 * 오케스트레이터가 보내는 결제 명령.
 *
 * <p>코레오그래피에서 오가던 것은 "주문이 생겼다"는 <b>사실(event)</b>이었다.
 * 여기서 오가는 것은 "결제하라"는 <b>지시(command)</b>다. 사실은 듣는 쪽이 무엇을
 * 할지 스스로 정하지만, 지시는 보내는 쪽이 이미 정해 놓은 것이다. 그래서 이
 * 서비스는 앞에 재고 단계가 있었다는 사실도, 뒤에 무엇이 오는지도 알 필요가 없다.
 *
 * <p>{@code amount} 를 이 서비스가 다시 계산하지 않고 명령에 담아 받는다.
 * 가격은 product-service 의 것이므로 여기서 알 방법이 없고, 주문 시점에 확정된
 * 금액으로 결제해야 하기 때문이다.
 */
public record PaymentCommand(Long orderId, Long userId, BigDecimal amount, Action action) {

    public static final String TOPIC = "payment-command";

    /**
     * 지금은 CHARGE 뿐이다. 결제가 마지막 단계라 그 뒤에 실패할 단계가 없어
     * 보상(REFUND)이 호출될 경로가 없기 때문이다. 배송 같은 단계를 뒤에 붙이는
     * 시점에 추가한다.
     */
    public enum Action {
        CHARGE
    }
}

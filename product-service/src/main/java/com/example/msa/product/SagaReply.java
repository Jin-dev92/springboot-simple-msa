package com.example.msa.product;

/**
 * 참여자가 오케스트레이터에게 돌려주는 응답. 모든 참여자가 같은 토픽으로 답한다.
 *
 * <p>{@code action} 을 열거형이 아니라 문자열로 둔 이유가 있다. 이 필드에는
 * 이 서비스의 RESERVE/RELEASE 와 payment-service 의 CHARGE 가 모두 실린다.
 * 열거형으로 만들려면 세 값을 모두 아는 공통 타입이 필요하고, 그러면 서비스 간
 * 공통 모듈을 두지 않는다는 이 저장소의 방침이 깨진다.
 */
public record SagaReply(Long orderId, String action, boolean success, String reason) {

    public static final String TOPIC = "saga-reply";

    static SagaReply ok(Long orderId, StockCommand.Action action) {
        return new SagaReply(orderId, action.name(), true, null);
    }

    static SagaReply fail(Long orderId, StockCommand.Action action, String reason) {
        return new SagaReply(orderId, action.name(), false, reason);
    }
}

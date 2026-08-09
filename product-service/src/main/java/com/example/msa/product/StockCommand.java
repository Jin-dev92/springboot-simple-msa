package com.example.msa.product;

/**
 * 오케스트레이터가 보내는 재고 명령.
 *
 * <p>코레오그래피에서 이 서비스가 받던 것은 "주문이 생겼다"는 <b>사실(event)</b>이었고,
 * 재고를 잡을지 말지는 이 서비스가 스스로 판단했다. 이제 받는 것은 "재고를 잡아라",
 * "되돌려라"라는 <b>지시(command)</b>다. 판단은 오케스트레이터가 한다.
 *
 * <p>실행({@code RESERVE})과 보상({@code RELEASE})을 같은 토픽에 담는 것이 중요하다.
 * Kafka 는 같은 토픽의 같은 파티션 안에서만 순서를 보장한다. 토픽을 나누면
 * "되돌려라"가 "잡아라"를 추월할 수 있어, 그 경합을 애플리케이션이 직접 막아야 한다.
 */
public record StockCommand(Long orderId, Long productId, int quantity, Action action) {

    public static final String TOPIC = "stock-command";

    public enum Action {
        RESERVE,
        RELEASE
    }
}

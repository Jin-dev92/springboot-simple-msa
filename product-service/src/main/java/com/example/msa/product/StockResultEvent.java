package com.example.msa.product;

/**
 * 재고 처리 결과를 알리는 이벤트. Saga 의 두 번째 구간이다.
 *
 * <p>OrderCreatedEvent 가 "주문이 생겼다"였다면 이것은 "재고를 잡았다 / 못 잡았다"는
 * 사실이다. order-service 는 이 사실을 듣고 주문을 확정하거나 취소한다.
 *
 * <p>성공과 실패를 별도 토픽으로 나누지 않고 {@code reserved} 값으로 구분한다.
 * 듣는 쪽이 하나뿐이고 두 경우 모두 같은 곳에서 처리되므로, 토픽을 늘리는 대신
 * 필드 하나로 해결했다. 구독자가 늘어나 성공에만 관심 있는 쪽이 생기면
 * 그때 토픽을 나누는 편이 낫다.
 */
public record StockResultEvent(Long orderId, Long productId, int quantity,
                               boolean reserved, String reason) {

    public static final String TOPIC = "stock-result";

    static StockResultEvent reserved(Long orderId, Long productId, int quantity) {
        return new StockResultEvent(orderId, productId, quantity, true, null);
    }

    static StockResultEvent rejected(Long orderId, Long productId, int quantity, String reason) {
        return new StockResultEvent(orderId, productId, quantity, false, reason);
    }
}

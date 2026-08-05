package com.example.msa.order;

/**
 * 주문이 생성되었다는 사실을 알리는 이벤트.
 *
 * <p>이름이 {@code DecreaseStockCommand} 가 아니라는 점이 중요하다. order-service 는
 * "재고를 깎아라"라고 지시하지 않고 "주문이 생겼다"는 사실만 알린다. 그 사실을 듣고
 * 무엇을 할지는 듣는 쪽이 정한다. 그래서 나중에 알림 서비스나 통계 서비스가 같은
 * 이벤트를 구독해도 order-service 는 손댈 필요가 없다.
 */
public record OrderCreatedEvent(Long orderId, Long productId, int quantity) {

    /** 토픽 이름. product-service 도 같은 문자열을 알고 있어야 한다. */
    public static final String TOPIC = "order-created";
}

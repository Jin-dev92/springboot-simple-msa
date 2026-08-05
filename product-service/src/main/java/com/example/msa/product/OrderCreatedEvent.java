package com.example.msa.product;

/**
 * order-service 가 발행하는 이벤트를 받기 위한 수신측 정의.
 *
 * <p>order-service 에 같은 이름의 record 가 따로 있다. ProductResponse 와 마찬가지로
 * 공유하지 않고 각자 선언한다. 두 서비스가 합의한 것은 자바 클래스가 아니라
 * <b>JSON 필드 이름</b>이며, 그것만 맞으면 서로 다른 언어로 짜여 있어도 통한다.
 *
 * <p>필드를 전부 받을 필요도 없다. 여기서 orderId 는 로그용으로만 쓰이고, 실제로
 * 필요한 것은 productId 와 quantity 뿐이다.
 */
public record OrderCreatedEvent(Long orderId, Long productId, int quantity) {

    public static final String TOPIC = "order-created";
}

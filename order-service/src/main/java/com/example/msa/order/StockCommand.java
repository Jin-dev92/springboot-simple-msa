package com.example.msa.order;

/**
 * 재고 참여자에게 보내는 명령.
 *
 * <p>product-service 에 같은 모양의 record 가 따로 있다. 공통 모듈로 묶지 않는 것은
 * 이 저장소의 방침이다. 발신측이 클래스 이름을 헤더에 담지 않으므로
 * ({@code spring.json.add.type.headers: false}) 필드 이름만 맞으면 된다.
 * 서비스가 서로의 클래스에 묶이지 않아야 독립 배포가 가능해진다.
 *
 * <p>실행(RESERVE)과 보상(RELEASE)을 같은 토픽에 담는다. Kafka 는 같은 토픽의 같은
 * 파티션 안에서만 순서를 보장하므로, 토픽을 나누면 "되돌려라"가 "잡아라"를 추월할
 * 수 있다. 발행할 때 orderId 를 메시지 키로 지정해 같은 파티션에 들어가게 한다.
 */
record StockCommand(Long orderId, Long productId, int quantity, Action action) {

    static final String TOPIC = "stock-command";

    enum Action {
        RESERVE,
        RELEASE
    }
}

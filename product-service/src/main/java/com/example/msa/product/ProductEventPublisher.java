package com.example.msa.product;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * 상품 변경 사실을 outbox 에 기록한다. 실제 발행은 {@link OutboxRelay} 가 맡는다.
 *
 * <p>호출하는 쪽이 전부 {@code @Transactional} 안이므로, <b>재고 변경과 이 이벤트가
 * 한 트랜잭션에 함께 커밋</b>된다. 재고는 깎였는데 이벤트가 안 나간 상태가 생기지
 * 않는다.
 *
 * <p>이 서비스에 outbox 가 필요해진 이유가 order-service 때와 다르다는 점이 중요하다.
 * 거기서는 명령이 유실되면 사가가 멈추지만, <b>스위퍼가 걷어내 주었다</b>. 여기서는
 * 이벤트를 놓치면 구독자의 복제본이 <b>영원히 낡은 값을 들고 있게</b> 된다. 아무도
 * 그것을 알아채지 못하고 자기 교정 장치도 없다. 복제본을 두는 순간 발행 신뢰성이
 * 선택이 아니라 전제가 된다.
 */
@Component
class ProductEventPublisher {

    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    ProductEventPublisher(OutboxRepository outbox, ObjectMapper objectMapper) {
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    void productChanged(Product product) {
        ProductChangedEvent event = ProductChangedEvent.of(product);
        try {
            outbox.save(new OutboxMessage(ProductChangedEvent.TOPIC,
                    String.valueOf(product.getId()), objectMapper.writeValueAsString(event)));
        } catch (JsonProcessingException e) {
            // 직렬화 실패는 재시도로 풀리지 않는다. 트랜잭션을 되돌려 재고 변경까지
            // 함께 취소한다. 재고만 바뀌고 이벤트가 없는 상태를 만들지 않기 위해서다.
            throw new IllegalStateException(
                    "상품 변경 이벤트 직렬화 실패: productId=" + product.getId(), e);
        }
    }
}

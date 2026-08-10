package com.example.msa.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 상품 변경 사실을 받아 로컬 복제본을 갱신한다.
 *
 * <p>이벤트가 상품의 현재 모습 전체를 싣고 오므로 <b>있으면 덮어쓰고 없으면 만든다</b>
 * (upsert). 순서가 뒤바뀌지 않는 한 이것만으로 원본과 같아진다. 같은 상품의 이벤트는
 * productId 를 메시지 키로 써서 같은 파티션에 들어가므로 순서가 지켜진다.
 *
 * <p>{@code containerFactory} 를 따로 지정한 이유가 있다. 이 서비스는 이제 두 종류의
 * 메시지를 구독한다 — {@code saga-reply} 와 {@code product-changed}. 발신측이 타입
 * 헤더를 붙이지 않으므로 수신측이 타입을 정해야 하는데, 전역 설정
 * ({@code spring.json.value.default.type})은 하나뿐이다. 그래서 이 리스너만 다른
 * 역직렬화기를 쓰는 컨테이너 팩토리를 {@link KafkaConsumerConfig} 에 두었다.
 */
@Component
class ProductChangedListener {

    private static final Logger log = LoggerFactory.getLogger(ProductChangedListener.class);

    private final ProductReplicaRepository replicas;

    ProductChangedListener(ProductReplicaRepository replicas) {
        this.replicas = replicas;
    }

    @KafkaListener(topics = ProductChangedEvent.TOPIC, groupId = "order-service-product-replica",
            containerFactory = "productChangedListenerContainerFactory")
    @Transactional
    public void handle(ProductChangedEvent event) {
        replicas.findById(event.productId())
                .ifPresentOrElse(
                        replica -> replica.apply(event.name(), event.price(), event.stock()),
                        () -> replicas.save(new ProductReplica(event.productId(), event.name(),
                                event.price(), event.stock())));
        log.debug("상품 복제본 갱신: productId={}, 재고={}", event.productId(), event.stock());
    }
}

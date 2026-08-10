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
        ProductReplica replica = replicas.findById(event.productId()).orElse(null);
        if (replica == null) {
            replicas.save(new ProductReplica(event.productId(), event.name(), event.price(),
                    event.stock(), event.version()));
            log.debug("상품 복제본 생성: productId={}, version={}", event.productId(), event.version());
            return;
        }

        if (replica.alreadyApplied(event.version())) {
            // 중복 전달이거나 순서가 뒤바뀐 것이다. 덮어쓰면 최신 값이 옛 값으로 되돌아간다.
            log.debug("이미 반영한 순번이라 무시: productId={}, 받은={}, 보유={}",
                    event.productId(), event.version(), replica.getVersion());
            return;
        }

        if (replica.hasGapBefore(event.version())) {
            // 값 자체는 이 이벤트로 맞춰진다(전체 상태를 싣기 때문에). 그래도 유실이
            // 일어나고 있다는 신호이므로 남긴다. 마지막 이벤트를 놓치면 다음 것이 오지
            // 않아 영원히 낡으므로, 이 로그가 그 전조가 된다.
            log.warn("상품 변경 이벤트를 놓쳤다: productId={}, 보유={}, 받은={} ({}건 누락)",
                    event.productId(), replica.getVersion(), event.version(),
                    event.version() - replica.getVersion() - 1);
        }

        replica.apply(event.name(), event.price(), event.stock(), event.version());
        log.debug("상품 복제본 갱신: productId={}, 재고={}, version={}",
                event.productId(), event.stock(), event.version());
    }
}

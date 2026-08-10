package com.example.msa.order;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * {@code product_replica} 를 토픽 처음부터 다시 세운다.
 *
 * <p><b>왜 필요한가.</b> 복제본은 자기 교정이 되지 않는다(14절). 한 번 어긋나면 스스로
 * 돌아오지 못하므로, 되돌릴 수단을 따로 두어야 한다. 그 수단이 재구축이다.
 *
 * <p>이 프로젝트에서 재구축이 <b>미래의 과제가 아니라 지금의 결함</b>이라는 것을 실제로
 * 확인했다. {@code docker compose restart order-service} 한 번이면 재현된다.
 *
 * <pre>
 *   재시작 전:  모니터 currentStock = 11
 *   재시작 후:  모니터 currentStock = null      ← 복제본이 비었다
 *   원본:       모니터 stock = 11               ← 원본은 멀쩡하다
 *   오프셋:     CURRENT 2 / END 2, LAG 0        ← 다시 읽지 않는다
 * </pre>
 *
 * <p>원인은 <b>수명 불일치</b>다. 복제본은 서비스와 함께 죽지만(인메모리 H2) 컨슈머
 * 그룹 오프셋은 브로커에 남는다. 빈 테이블과 "이미 다 읽었다"는 기록이 만나면 그 상태로
 * 굳는다. 영속 DB 를 쓰더라도 복제본만 잘못 지워지면 같은 일이 벌어진다.
 *
 * <p><b>리스너의 컨슈머 그룹을 건드리지 않는다.</b> 그룹을 공유하면 재구축이 평상시
 * 소비와 얽히고 오프셋을 되감는 부작용이 남는다. 대신 {@code assign} 으로 파티션을
 * 직접 잡아 읽는다 — 그룹 관리도 오프셋 커밋도 일어나지 않으므로, 몇 번을 돌려도
 * 평상시 흐름에 흔적을 남기지 않는다.
 */
@Component
class ProductReplicaRebuilder {

    private static final Logger log = LoggerFactory.getLogger(ProductReplicaRebuilder.class);

    /** 한 번의 poll 이 기다리는 시간. 끝에 도달했는지는 오프셋으로 판단한다. */
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final ConsumerFactory<String, ProductChangedEvent> consumerFactory;
    private final ProductReplicaRepository replicas;

    ProductReplicaRebuilder(
            ConsumerFactory<String, ProductChangedEvent> productChangedConsumerFactory,
            ProductReplicaRepository replicas) {
        this.consumerFactory = productChangedConsumerFactory;
        this.replicas = replicas;
    }

    /**
     * 토픽을 처음부터 읽어 복제본을 다시 세운다.
     *
     * @return 반영한 상품 수
     */
    // public 인 이유는 다른 @Transactional 메서드들과 같다. 프록시를 거쳐야 트랜잭션이
    // 실제로 걸린다.
    @Transactional
    public int rebuild() {
        Map<Long, ProductChangedEvent> latest = readAllFromBeginning();

        // 읽는 동안 같은 상품이 여러 번 나왔다면 마지막 것만 남는다. 이벤트가 상품의
        // 현재 모습 전체를 싣기 때문에(14절) 중간 것을 버려도 결과가 같다.
        latest.values().forEach(event -> replicas.findById(event.productId())
                .ifPresentOrElse(
                        replica -> replica.apply(event.name(), event.price(), event.stock()),
                        () -> replicas.save(new ProductReplica(event.productId(), event.name(),
                                event.price(), event.stock()))));

        log.info("상품 복제본 재구축 완료: {}건", latest.size());
        return latest.size();
    }

    /**
     * 토픽의 모든 파티션을 처음부터 끝까지 읽어 <b>키별 최신 이벤트</b>만 남긴다.
     *
     * <p>토픽이 압축(compact)되어 있으므로 대개 상품 수만큼만 읽힌다. 압축이 아직
     * 돌지 않아 중복이 남아 있어도 결과는 같다 — 뒤에 온 것이 앞의 것을 덮는다.
     */
    private Map<Long, ProductChangedEvent> readAllFromBeginning() {
        // 그룹 id 를 주지만 assign 을 쓰므로 그룹 관리에 참여하지 않는다.
        // 오프셋도 커밋하지 않아 리스너 쪽 진행 상태에 영향이 없다.
        try (Consumer<String, ProductChangedEvent> consumer =
                consumerFactory.createConsumer("product-replica-rebuild", null)) {

            List<TopicPartition> partitions = consumer.partitionsFor(ProductChangedEvent.TOPIC)
                    .stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            if (partitions.isEmpty()) {
                log.warn("재구축할 토픽이 없다: {}", ProductChangedEvent.TOPIC);
                return Map.of();
            }

            consumer.assign(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            consumer.seekToBeginning(partitions);

            // 삽입 순서를 유지해 로그를 읽기 쉽게 한다.
            Map<Long, ProductChangedEvent> latest = new LinkedHashMap<>();
            List<TopicPartition> remaining = new ArrayList<>(partitions);
            while (!remaining.isEmpty()) {
                var records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty() && allReachedEnd(consumer, remaining, ends)) {
                    break;
                }
                for (ConsumerRecord<String, ProductChangedEvent> record : records) {
                    ProductChangedEvent event = record.value();
                    if (event != null) {
                        latest.put(event.productId(), event);
                    }
                }
                remaining.removeIf(p -> consumer.position(p) >= ends.getOrDefault(p, 0L));
            }
            return latest;
        }
    }

    private static boolean allReachedEnd(Consumer<String, ProductChangedEvent> consumer,
            List<TopicPartition> partitions, Map<TopicPartition, Long> ends) {
        return partitions.stream()
                .allMatch(p -> consumer.position(p) >= ends.getOrDefault(p, 0L));
    }
}

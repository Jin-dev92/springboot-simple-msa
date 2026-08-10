package com.example.msa.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 기동 시 복제본이 비어 있으면 재구축한다. 실제로 확인된 결함을 막는 자리다.
 *
 * <p>{@code docker compose restart order-service} 하나로 재현된다. 인메모리 H2 는
 * 재시작하면 비워지지만 컨슈머 그룹 오프셋은 브로커에 남아 있어, 리스너는 "이미 다
 * 읽었다"고 판단하고 아무것도 다시 주지 않는다. <b>빈 복제본이 그대로 굳는다.</b>
 *
 * <p>복제본이 이미 차 있으면 건너뛴다. 정상 재기동마다 전체를 다시 읽을 이유가 없다.
 *
 * <p>실패해도 기동을 막지 않는다. 브로커가 아직 안 떴을 뿐일 수 있고, 그때 서비스를
 * 통째로 못 뜨게 하는 것은 과하다. 대신 경고를 남긴다 — 복제본이 빈 채로 도는 것은
 * 조용히 넘어가면 안 되는 상태다.
 */
@Component
@ConditionalOnProperty(name = "replica.rebuild-on-startup", havingValue = "true",
        matchIfMissing = true)
class ProductReplicaStartupRebuild {

    private static final Logger log =
            LoggerFactory.getLogger(ProductReplicaStartupRebuild.class);

    private final ProductReplicaRepository replicas;
    private final ProductReplicaRebuilder rebuilder;

    ProductReplicaStartupRebuild(ProductReplicaRepository replicas,
            ProductReplicaRebuilder rebuilder) {
        this.replicas = replicas;
        this.rebuilder = rebuilder;
    }

    @EventListener(ApplicationReadyEvent.class)
    void rebuildIfEmpty() {
        if (replicas.count() > 0) {
            return;
        }
        try {
            int restored = rebuilder.rebuild();
            log.info("복제본이 비어 있어 재구축했다: {}건", restored);
        } catch (Exception e) {
            log.warn("기동 시 복제본 재구축에 실패했다. 복제본이 빈 채로 동작한다 "
                    + "— 관리 엔드포인트로 다시 시도할 수 있다", e);
        }
    }
}

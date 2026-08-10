package com.example.msa.order;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 복제본이 원본과 같은지 주기적으로 대조한다.
 *
 * <p>15절에서 재구축 수단을 만들었지만 <b>틀렸다는 것을 알아채는 장치</b>는 없었다.
 * 사람이 눈치채야 재구축을 부를 수 있었다. 이 클래스가 그 자리를 메운다.
 *
 * <p><b>가장 어려운 부분은 비교가 아니라 오탐 처리다.</b> 결과적 일관성 시스템에서는
 * 정상 상태에서도 두 값이 다르다.
 *
 * <pre>
 *   원본 조회 (재고 11) ── 그 사이 주문 발생 ── 복제본 조회 (재고 11, 아직 못 받음)
 *                                              → "불일치"   ← 오탐
 * </pre>
 *
 * <p>이걸 그대로 경보로 올리면 늘 울려서 아무도 보지 않게 된다. 그래서 판정 기준을
 * 이렇게 둔다.
 *
 * <blockquote><b>단발 불일치는 정상이다. 지속되는 불일치만 결함이다.</b></blockquote>
 *
 * <p>연속 {@code threshold} 회 다를 때만 경고한다. 일시적 지연이면 다음 회차에
 * 사라지고, 진짜로 어긋났으면 계속 남는다. 검사 주기를 복제 지연보다 넉넉히 크게
 * 잡는 것이 전제다.
 *
 * <p><b>자동 재구축은 하지 않는다.</b> 수단은 이미 있지만(15절) 오탐에 반응해 재구축을
 * 반복하면 원본에 부하만 준다. 감시가 충분히 정확하다고 확인된 뒤에 연결할 일이며,
 * 그전까지는 사람이 로그를 보고 관리 엔드포인트로 부르는 편이 안전하다.
 */
@Component
class ProductReplicaVerifier {

    private static final Logger log = LoggerFactory.getLogger(ProductReplicaVerifier.class);

    private final ProductReplicaRepository replicas;
    private final ProductClient productClient;
    private final int threshold;

    /** 연속 불일치 횟수. 한 번이라도 일치하면 0 으로 되돌린다. */
    private final AtomicInteger consecutiveMismatches = new AtomicInteger();

    ProductReplicaVerifier(ProductReplicaRepository replicas, ProductClient productClient,
            @Value("${replica.verify.mismatch-threshold}") int threshold) {
        this.replicas = replicas;
        this.productClient = productClient;
        this.threshold = threshold;
    }

    @Scheduled(fixedDelayString = "${replica.verify.interval}")
    @Transactional(readOnly = true)
    public void verify() {
        ProductClient.ChecksumResponse origin = productClient.checksum();
        if (origin == null) {
            // 상대가 죽어 있거나 회로가 열렸다. "틀렸다"가 아니라 "확인할 수 없다"이므로
            // 불일치로 세지 않는다. 확인 못 한 것을 결함으로 세면 상대의 장애가
            // 곧바로 거짓 경보가 된다.
            return;
        }

        List<ProductReplica> all = replicas.findAll();
        String local = ProductChecksum.of(all);

        if (local.equals(origin.checksum())) {
            int previous = consecutiveMismatches.getAndSet(0);
            if (previous > 0) {
                log.info("복제본이 다시 원본과 일치한다 (연속 불일치 {}회 뒤 회복)", previous);
            }
            return;
        }

        int count = consecutiveMismatches.incrementAndGet();
        if (count < threshold) {
            // 아직은 복제 지연일 수 있다. 다음 회차에 사라지면 정상이다.
            log.debug("복제본 불일치 {}회 (임계 {}). 아직 지연일 수 있다: 원본 {}건, 복제본 {}건",
                    count, threshold, origin.count(), all.size());
            return;
        }

        log.warn("복제본이 원본과 계속 다르다. 재구축이 필요하다 "
                        + "(연속 {}회, 원본 {}건/{}, 복제본 {}건/{})",
                count, origin.count(), origin.checksum(), all.size(), local);
    }

    /** 테스트와 운영 확인용. 지금까지 연속으로 몇 번 어긋났는지. */
    int consecutiveMismatches() {
        return consecutiveMismatches.get();
    }
}

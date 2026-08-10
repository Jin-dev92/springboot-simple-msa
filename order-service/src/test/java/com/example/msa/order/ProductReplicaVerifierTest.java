package com.example.msa.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * 검증기의 어려운 부분은 비교가 아니라 <b>오탐 처리</b>다. 그래서 그쪽을 집중해서 본다.
 *
 * <p>결과적 일관성 시스템에서는 정상 상태에서도 원본과 복제본이 다르다. 단발 불일치를
 * 결함으로 세면 경보가 늘 울려 아무도 보지 않게 된다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "saga.timeout.check-interval=1h",
        "outbox.poll-interval=1h",
        "replica.rebuild-on-startup=false",
        // 검증은 수동으로만 돌린다.
        "replica.verify.interval=1h",
        "replica.verify.mismatch-threshold=3"
})
class ProductReplicaVerifierTest {

    @Autowired
    private ProductReplicaVerifier verifier;

    @Autowired
    private ProductReplicaRepository replicas;

    @Autowired
    private ProductChangedListener listener;

    @MockitoBean
    private ProductClient productClient;

    @BeforeEach
    void 비운다() {
        replicas.deleteAll();
        // 연속 불일치 횟수는 싱글턴 빈의 상태라 테스트 사이에 남는다.
        // 프로덕션에 테스트용 초기화 메서드를 여는 대신, 한 번 일치시켜 0 으로 되돌린다.
        originReports(ProductChecksum.of(java.util.List.of()), 0);
        verifier.verify();
        assertThat(verifier.consecutiveMismatches()).isZero();
    }

    /** 복제본에 상품 하나를 넣고, 그 상태의 올바른 체크섬을 돌려준다. */
    private String seedOneProduct() {
        listener.handle(new ProductChangedEvent(1L, "키보드", new BigDecimal("89000"), 30, 1));
        return ProductChecksum.of(replicas.findAll());
    }

    private void originReports(String checksum, int count) {
        given(productClient.checksum())
                .willReturn(new ProductClient.ChecksumResponse(count, checksum));
    }

    @Test
    void 일치하면_불일치_횟수가_쌓이지_않는다() {
        originReports(seedOneProduct(), 1);

        verifier.verify();
        verifier.verify();

        assertThat(verifier.consecutiveMismatches()).isZero();
    }

    @Test
    void 임계에_이르기_전에는_경고하지_않고_횟수만_센다() {
        seedOneProduct();
        originReports("다른값", 1);

        verifier.verify();
        verifier.verify();

        // 임계는 3이다. 아직 복제 지연일 수 있으므로 결함으로 보지 않는다.
        assertThat(verifier.consecutiveMismatches()).isEqualTo(2);
    }

    @Test
    void 일시적_불일치는_한_번_일치하면_초기화된다() {
        String correct = seedOneProduct();
        originReports("다른값", 1);
        verifier.verify();
        verifier.verify();
        assertThat(verifier.consecutiveMismatches()).isEqualTo(2);

        // 복제가 따라잡았다.
        originReports(correct, 1);
        verifier.verify();

        // 이것이 오탐을 걸러내는 지점이다. 누적되지 않고 0 으로 돌아가야 한다.
        assertThat(verifier.consecutiveMismatches()).isZero();
    }

    @Test
    void 원본을_확인할_수_없으면_불일치로_세지_않는다() {
        seedOneProduct();
        // 서킷 브레이커 폴백이 null 을 돌려준 상황. "틀렸다"가 아니라 "모른다"이다.
        given(productClient.checksum()).willReturn(null);

        verifier.verify();
        verifier.verify();

        // 상대의 장애가 곧바로 거짓 경보가 되면 안 된다.
        assertThat(verifier.consecutiveMismatches()).isZero();
    }

    @Test
    void 지속되는_불일치는_임계를_넘어_계속_쌓인다() {
        seedOneProduct();
        originReports("다른값", 1);

        for (int i = 0; i < 4; i++) {
            verifier.verify();
        }

        // 진짜로 어긋난 경우는 사라지지 않는다. 임계를 넘어서면 경고가 나간다.
        assertThat(verifier.consecutiveMismatches()).isEqualTo(4);
    }
}

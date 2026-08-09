package com.example.msa.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Phase 11 의 검증 기준. <b>다른 서비스의 현재 상태로 정렬하고 페이징할 수 있는가.</b>
 *
 * <p>12절의 스냅샷으로는 답할 수 없는 질의다. 재고는 주문 시점 값이 아니라 지금
 * 값이어야 하기 때문이다. 상품 복제본을 이 DB 에 두었으므로 평범한 SQL 조인이 되고,
 * <b>정렬과 페이징이 DB 에서</b> 끝난다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
        "saga.timeout.check-interval=1h",
        "outbox.poll-interval=1h"
})
class OrderSummaryTest {

    private static final long USER = 77L;

    @Autowired
    private OrderRepository orders;

    @Autowired
    private ProductReplicaRepository replicas;

    @Autowired
    private ProductChangedListener listener;

    @MockitoBean
    private ProductClient productClient;

    @BeforeEach
    void 비운다() {
        orders.deleteAll();
        replicas.deleteAll();
    }

    private Order order(long productId, String productName) {
        return orders.save(new Order(USER, productId, productName, 1, new BigDecimal("1000")));
    }

    private void productChanged(long productId, String name, int stock) {
        listener.handle(new ProductChangedEvent(productId, name, new BigDecimal("1000"), stock));
    }

    @Test
    void 이벤트를_받으면_복제본이_생기고_다시_받으면_덮어쓴다() {
        productChanged(1L, "키보드", 30);
        assertThat(replicas.findById(1L).orElseThrow().getStock()).isEqualTo(30);

        productChanged(1L, "키보드", 27);

        // 행이 늘지 않고 덮어써져야 한다. 이벤트가 상품의 현재 모습 전체를 싣고 오므로
        // 통째로 덮어쓰면 되고, 그래서 몇 개를 놓쳐도 최신 것 하나로 수렴한다.
        assertThat(replicas.findAll()).hasSize(1);
        assertThat(replicas.findById(1L).orElseThrow().getStock()).isEqualTo(27);
    }

    @Test
    void 현재_재고가_적은_순으로_정렬된다() {
        order(1L, "키보드");
        order(2L, "모니터");
        order(3L, "마우스");
        productChanged(1L, "키보드", 30);
        productChanged(2L, "모니터", 12);
        productChanged(3L, "마우스", 50);

        var page = orders.findSummariesByUserId(USER, PageRequest.of(0, 10));

        // 주문한 순서(1,2,3)가 아니라 현재 재고 순(12, 30, 50)이어야 한다.
        assertThat(page.getContent())
                .extracting(OrderSummary::productName, OrderSummary::currentStock)
                .containsExactly(
                        org.assertj.core.api.Assertions.tuple("모니터", 12),
                        org.assertj.core.api.Assertions.tuple("키보드", 30),
                        org.assertj.core.api.Assertions.tuple("마우스", 50));
    }

    @Test
    void 페이징이_DB에서_처리된다() {
        for (long i = 1; i <= 3; i++) {
            order(i, "상품" + i);
            productChanged(i, "상품" + i, (int) i * 10);
        }

        var first = orders.findSummariesByUserId(USER, PageRequest.of(0, 2));
        var second = orders.findSummariesByUserId(USER, PageRequest.of(1, 2));

        assertThat(first.getTotalElements()).isEqualTo(3);
        assertThat(first.getContent()).hasSize(2);
        assertThat(second.getContent()).hasSize(1);

        // 페이지를 가로질러도 정렬이 이어져야 한다. 메모리에서 자른 것이 아니라
        // DB 가 정렬한 뒤 잘라 주었다는 뜻이다.
        assertThat(first.getContent().get(0).currentStock()).isEqualTo(10);
        assertThat(second.getContent().get(0).currentStock()).isEqualTo(30);
    }

    @Test
    void 복제본이_아직_없는_상품은_재고가_비고_뒤로_밀린다() {
        order(1L, "키보드");
        order(9L, "아직_복제_안된_상품");
        productChanged(1L, "키보드", 30);

        var page = orders.findSummariesByUserId(USER, PageRequest.of(0, 10));

        // 모르는 값을 "재고 0" 으로 취급해 맨 앞에 세우면 화면이 거짓말을 한다.
        assertThat(page.getContent()).extracting(OrderSummary::currentStock)
                .containsExactly(30, null);
    }

    @Test
    void 조회에_product_service_를_부르지_않는다() {
        order(1L, "키보드");
        productChanged(1L, "키보드", 30);

        var page = orders.findSummariesByUserId(USER, PageRequest.of(0, 10));

        // 복제본이 있으므로 상대가 죽어 있어도 이 화면은 뜬다. 재고가 그만큼 낡을 뿐이다.
        assertThat(page.getContent()).hasSize(1);
        verify(productClient, never()).findById(any());
    }

    @Test
    void 남의_주문은_보이지_않는다() {
        order(1L, "키보드");
        orders.save(new Order(999L, 1L, "키보드", 1, new BigDecimal("1000")));
        productChanged(1L, "키보드", 30);

        var page = orders.findSummariesByUserId(USER, PageRequest.of(0, 10));

        assertThat(page.getTotalElements()).isEqualTo(1);
    }
}

package com.example.msa.order;

import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OrderRepository extends JpaRepository<Order, Long> {

    /**
     * 본인 주문만 조회한다.
     *
     * <p>{@code findAll()} 을 쓰고 나중에 자바 코드에서 거르는 방식은 위험하다.
     * 거르는 코드를 빠뜨리면 그대로 전체 노출이 된다. 조회 단계에서부터 사용자를
     * 조건에 넣어, 실수로 남의 데이터를 가져올 여지 자체를 없앤다.
     */
    List<Order> findByUserId(Long userId);

    Optional<Order> findByIdAndUserId(Long id, Long userId);

    /**
     * 주문과 상품 복제본을 조인해 <b>현재 재고가 적은 순</b>으로 돌려준다.
     *
     * <p>이 질의가 Phase 11 의 존재 이유다. 정렬 기준인 재고가 product-service 의
     * <b>현재</b> 상태라, 12절의 스냅샷으로는 풀 수 없다. 복제본을 이 DB 안에 두었기
     * 때문에 비로소 평범한 SQL 조인이 되고, <b>정렬과 페이징이 DB 에서</b> 끝난다.
     *
     * <p>API Composition 이었다면 이렇게 해야 한다: 내 주문을 <b>전부</b> 가져오고,
     * 상품을 <b>전부</b> 가져와 메모리에서 붙인 뒤, 거기서 정렬하고 잘라낸다.
     * 정렬 기준이 상대 서비스에 있으면 페이징을 아래로 내려보낼 수 없기 때문이다.
     * 주문이 많아질수록 그대로 무너진다.
     *
     * <p>{@code left join} 인 이유는 복제본이 아직 도착하지 않은 상품이 있을 수 있기
     * 때문이다. 그때 재고는 {@code null} 이 되고, 정렬에서 뒤로 보낸다 — 모르는 값을
     * "재고 0" 으로 취급해 맨 앞에 세우면 화면이 거짓말을 한다.
     */
    @Query("""
            select new com.example.msa.order.OrderSummary(
                o.id, o.productId, o.productName, o.quantity, o.totalPrice, o.status,
                r.stock, r.updatedAt)
            from Order o
            left join ProductReplica r on r.productId = o.productId
            where o.userId = :userId
            order by r.stock asc nulls last, o.id desc
            """)
    Page<OrderSummary> findSummariesByUserId(@Param("userId") Long userId, Pageable pageable);
}

package com.example.msa.order;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * product-service 데이터의 <b>로컬 사본</b>. 조인을 가능하게 하려고 둔다.
 *
 * <p>12절의 스냅샷({@code Order.productName})과 정반대 성격이라는 점이 핵심이다.
 *
 * <pre>
 *   스냅샷  — 주문 시점의 값을 <b>박제</b>한다. 원본이 바뀌어도 따라가지 않는다.
 *   복제본  — 원본을 <b>계속 따라간다</b>. 이벤트가 올 때마다 덮어쓴다.
 * </pre>
 *
 * <p>둘 다 "다른 서비스 데이터를 내 DB에 둔다"지만 묻는 질문이 다르다. 과거의
 * 사실이면 박제하고, 지금의 상태면 따라가야 한다. 재고는 후자다 — "지금 품절인가"
 * 는 주문 시점 재고로 답할 수 없다.
 *
 * <p>이 테이블이 생겨서 얻는 것은 하나다. <b>order-service 의 DB 안에서 평범한 SQL
 * 조인이 된다.</b> 그래서 현재 재고로 정렬하고 페이징할 수 있다. API Composition
 * 으로는 정렬 기준이 상대 서비스에 있어 전부 메모리로 끌어와야 하는 일이다.
 *
 * <p>대가는 결과적 일관성이다. 이벤트가 도착하기 전까지 이 값은 낡아 있다. 그리고
 * 이벤트를 <b>놓치면 영원히</b> 낡은 채로 남는다 — 재고처럼 자기 교정되지 않는다.
 * 그래서 product-service 쪽에 Outbox 가 먼저 필요했다.
 */
@Entity
@Table(name = "product_replica")
public class ProductReplica {

    /** 원본의 id 를 그대로 쓴다. 사본이므로 자체 식별자를 만들 이유가 없다. */
    @Id
    private Long productId;

    private String name;

    private BigDecimal price;

    private int stock;

    /** 마지막으로 갱신된 시각. 얼마나 낡았는지를 눈으로 보기 위해 남긴다. */
    private Instant updatedAt;

    protected ProductReplica() {
        // JPA 기본 생성자
    }

    ProductReplica(Long productId, String name, BigDecimal price, int stock) {
        this.productId = productId;
        apply(name, price, stock);
    }

    /**
     * 이벤트의 내용으로 통째로 덮어쓴다.
     *
     * <p>바뀐 필드만 골라 갱신하지 않는 이유는 이벤트가 상품의 현재 모습 전체를
     * 싣고 오기 때문이다. 덮어쓰기만 하면 되므로 <b>이벤트를 몇 개 놓쳤다가 최신
     * 것 하나만 받아도 결국 맞는 값으로 수렴</b>한다.
     */
    void apply(String name, BigDecimal price, int stock) {
        this.name = name;
        this.price = price;
        this.stock = stock;
        this.updatedAt = Instant.now();
    }

    public Long getProductId() {
        return productId;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public int getStock() {
        return stock;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}

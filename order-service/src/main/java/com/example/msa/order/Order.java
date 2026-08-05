package com.example.msa.order;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;

// ORDER 는 SQL 예약어(ORDER BY)이므로 테이블 이름을 orders 로 바꿔 준다.
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // product-service 의 데이터를 외래키로 묶지 않고 id 값만 들고 있는다.
    // 서비스마다 DB 가 분리되어 있으므로 DB 차원의 join 이나 제약조건은 존재할 수 없다.
    private Long productId;

    private int quantity;

    private BigDecimal totalPrice;

    protected Order() {
        // JPA 기본 생성자
    }

    public Order(Long productId, int quantity, BigDecimal totalPrice) {
        this.productId = productId;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
    }

    public Long getId() {
        return id;
    }

    public Long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getTotalPrice() {
        return totalPrice;
    }
}

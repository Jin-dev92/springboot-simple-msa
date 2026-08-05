package com.example.msa.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
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

    // 누구의 주문인지. JWT 의 uid 클레임에서 가져온다.
    // 이 값이 없으면 "인증된 사용자"까지만 알 뿐 "본인 것인지"는 판단할 수 없다.
    private Long userId;

    // product-service 의 데이터를 외래키로 묶지 않고 id 값만 들고 있는다.
    // 서비스마다 DB 가 분리되어 있으므로 DB 차원의 join 이나 제약조건은 존재할 수 없다.
    private Long productId;

    private int quantity;

    private BigDecimal totalPrice;

    /**
     * 주문 상태. Saga 가 어디까지 진행됐는지를 나타낸다.
     *
     * <p>PENDING 이 존재한다는 것이 핵심이다. 모놀리식이라면 재고 차감까지 한 트랜잭션에서
     * 끝나므로 주문은 성공 아니면 실패 둘 중 하나였다. 서비스를 나눈 뒤에는
     * <b>"주문은 받았지만 아직 확정되지 않은" 중간 상태</b>가 반드시 생긴다.
     */
    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    /** 취소된 경우 그 이유. 사용자에게 왜 취소됐는지 알려주기 위해 남긴다. */
    private String cancelReason;

    protected Order() {
        // JPA 기본 생성자
    }

    public Order(Long userId, Long productId, int quantity, BigDecimal totalPrice) {
        this.userId = userId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
        this.status = OrderStatus.PENDING;
    }

    /** 재고를 잡았다는 통보를 받았을 때. */
    void confirm() {
        this.status = OrderStatus.CONFIRMED;
    }

    /**
     * 재고를 못 잡았다는 통보를 받았을 때의 <b>보상</b>.
     *
     * <p>여기서는 상태만 바꾸면 되지만, 결제까지 있는 시스템이라면 환불을,
     * 쿠폰을 썼다면 쿠폰 복구를 함께 해야 한다. 보상은 "되돌리기"가 아니라
     * <b>되돌리는 효과를 내는 새로운 작업</b>이다. 이미 커밋된 트랜잭션은
     * 롤백할 수 없기 때문이다.
     */
    void cancel(String reason) {
        this.status = OrderStatus.CANCELLED;
        this.cancelReason = reason;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public String getCancelReason() {
        return cancelReason;
    }

    public Long getId() {
        return id;
    }

    public Long getUserId() {
        return userId;
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

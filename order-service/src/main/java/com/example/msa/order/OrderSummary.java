package com.example.msa.order;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 주문 + 상품의 <b>현재</b> 재고를 함께 담은 조회 결과.
 *
 * <p>{@code productName} 은 주문 시점에 박제된 값이고({@code Order.productName}),
 * {@code currentStock} 은 복제본에서 온 지금 값이다. <b>한 줄 안에 과거의 사실과
 * 현재의 상태가 함께 들어 있다.</b> 12절의 두 가지 성격이 여기서 만난다.
 *
 * <p>{@code stockUpdatedAt} 을 함께 내보내는 이유는 복제본이 얼마나 낡았는지를
 * 숨기지 않기 위해서다. 결과적 일관성을 쓰는 화면은 그 사실을 드러내는 편이 낫다.
 */
public record OrderSummary(Long orderId, Long productId, String productName, int quantity,
                           BigDecimal totalPrice, OrderStatus status,
                           Integer currentStock, Instant stockUpdatedAt) {
}

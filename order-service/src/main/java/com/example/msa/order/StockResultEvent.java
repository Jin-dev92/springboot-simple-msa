package com.example.msa.order;

/**
 * product-service 가 발행하는 재고 처리 결과. 발신측과 필드 이름만 맞추면 되므로
 * 클래스는 각자 선언한다.
 */
public record StockResultEvent(Long orderId, Long productId, int quantity,
                               boolean reserved, String reason) {

    public static final String TOPIC = "stock-result";
}

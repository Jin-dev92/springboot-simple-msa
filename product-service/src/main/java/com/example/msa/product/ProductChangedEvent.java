package com.example.msa.product;

import java.math.BigDecimal;

/**
 * 상품이 바뀌었다는 <b>사실</b>. 명령이 아니라 이벤트다.
 *
 * <p>여기서 오가는 것이 지시가 아니라는 점이 {@link StockCommand} 와 다르다.
 * 이 서비스는 누가 이걸 듣는지, 듣고 무엇을 하는지 모른다. 구독자가 늘어도
 * 이 코드는 바뀌지 않는다 — 11절에서 정리한 코레오그래피의 장점이 그대로 적용되는
 * 자리다. 흐름을 조정할 필요가 없는 <b>단순 전파</b>이기 때문이다.
 *
 * <p>바뀐 필드만 싣지 않고 <b>상품의 현재 모습 전체</b>를 싣는다. 받는 쪽이 이전
 * 상태를 몰라도 그대로 덮어쓰면 되므로(upsert) 구독자 구현이 단순해지고, 이벤트를
 * 몇 개 놓쳤다가 최신 것 하나만 받아도 결국 맞는 값으로 수렴한다.
 */
public record ProductChangedEvent(Long productId, String name, BigDecimal price, int stock) {

    public static final String TOPIC = "product-changed";

    static ProductChangedEvent of(Product product) {
        return new ProductChangedEvent(product.getId(), product.getName(),
                product.getPrice(), product.getStock());
    }
}

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
 * <p>{@code version} 은 이 상품의 변경 순번이다. 받는 쪽이 <b>사이에 놓친 이벤트가
 * 있는지</b> 알아채는 데 쓴다. 이벤트가 전체 상태를 싣기 때문에 놓쳐도 다음 것 하나로
 * 복구되지만, <b>마지막 이벤트를 놓치면 다음 것이 오지 않아 영원히 낡는다.</b> 그래서
 * 놓쳤다는 사실 자체를 아는 것이 값을 한다.
 *
 * <p>바뀐 필드만 싣지 않고 <b>상품의 현재 모습 전체</b>를 싣는다. 받는 쪽이 이전
 * 상태를 몰라도 그대로 덮어쓰면 되므로(upsert) 구독자 구현이 단순해지고, 이벤트를
 * 몇 개 놓쳤다가 최신 것 하나만 받아도 결국 맞는 값으로 수렴한다.
 */
public record ProductChangedEvent(Long productId, String name, BigDecimal price, int stock,
                                  long version) {

    public static final String TOPIC = "product-changed";

    static ProductChangedEvent of(Product product) {
        return new ProductChangedEvent(product.getId(), product.getName(),
                product.getPrice(), product.getStock(), product.getVersion());
    }
}

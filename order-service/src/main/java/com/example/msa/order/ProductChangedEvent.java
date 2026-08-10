package com.example.msa.order;

import java.math.BigDecimal;

/**
 * product-service 가 발행하는 상품 변경 사실. 이 서비스는 <b>구독만</b> 한다.
 *
 * <p>product-service 에 같은 모양의 record 가 따로 있다. 공통 모듈을 두지 않는 방침
 * 그대로이며, 발신측이 클래스 이름을 헤더에 담지 않으므로 필드 이름만 맞으면 된다.
 *
 * <p>명령({@link StockCommand})과 달리 이것은 <b>사실</b>이다. 보내는 쪽은 누가 듣는지
 * 모르고, 듣는 쪽이 무엇을 할지 스스로 정한다. 여기서는 복제본을 갱신한다.
 */
public record ProductChangedEvent(Long productId, String name, BigDecimal price, int stock,
                                  long version) {

    static final String TOPIC = "product-changed";
}

package com.example.msa.product;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * 재고 증감 규칙만 따로 확인한다. 스프링 컨텍스트가 필요 없는 순수 단위 테스트다.
 */
class ProductStockTest {

    private static Product product(int stock) {
        return new Product("테스트상품", new BigDecimal("1000"), stock);
    }

    @Test
    void 재고보다_많이_차감하면_실패하고_재고는_그대로다() {
        Product product = product(10);

        assertThat(product.decreaseStock(11)).isFalse();
        assertThat(product.getStock()).isEqualTo(10);
    }

    @Test
    void 차감한_만큼_되돌리면_원래_수량이_된다() {
        Product product = product(10);
        product.decreaseStock(4);

        product.increaseStock(4);

        assertThat(product.getStock()).isEqualTo(10);
    }
}

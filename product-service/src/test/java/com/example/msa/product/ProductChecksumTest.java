package com.example.msa.product;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * 체크섬 계산 규칙을 <b>고정</b>한다.
 *
 * <p>이 규칙은 order-service 의 같은 이름 클래스와 반드시 일치해야 하는데, 공통 모듈을
 * 두지 않는 방침 때문에 코드로 묶여 있지 않다. 그래서 양쪽 테스트에 <b>같은 입력의
 * 같은 기대값</b>을 박아 둔다. 한쪽 규칙이 바뀌면 그쪽 빌드가 깨져 드러난다.
 *
 * <p>{@code order-service/.../ProductChecksumTest} 의 기대값과 같아야 한다.
 */
class ProductChecksumTest {

    /** 두 서비스가 공유해야 하는 고정 기대값. 바꾸려면 양쪽을 함께 바꿔야 한다. */
    static final String EXPECTED =
            "3b435ff9c16bef497f797f4b18d7c0161e3d5fced87441481e51e37008d6d11d";

    @Test
    void 고정된_입력은_고정된_체크섬을_낸다() {
        String input = ProductChecksum.line(1L, "키보드", new BigDecimal("89000.00"), 30, 1)
                + ProductChecksum.line(2L, "모니터", new BigDecimal("320000"), 12, 3);

        assertThat(ProductChecksum.sha256(input)).isEqualTo(EXPECTED);
    }

    @Test
    void 금액의_소수점_표기가_달라도_같은_값으로_본다() {
        // DB 에서 읽어 오면 89000 이 되기도 89000.00 이 되기도 한다.
        // 정규화하지 않으면 값이 같은데 체크섬만 달라 영원히 불일치로 나온다.
        assertThat(ProductChecksum.normalize(new BigDecimal("89000")))
                .isEqualTo(ProductChecksum.normalize(new BigDecimal("89000.00")));
    }
}

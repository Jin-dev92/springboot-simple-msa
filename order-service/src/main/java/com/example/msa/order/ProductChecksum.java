package com.example.msa.order;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * 상품 전체를 한 값으로 요약한다. 구독자가 자기 복제본과 대조하는 데 쓴다.
 *
 * <p>전부 내려받아 필드별로 비교해도 되지만, 그러면 상품 수만큼 데이터가 오간다.
 * 요약값 하나면 <b>왕복 한 번으로 전체가 같은지</b> 알 수 있다. 대신 다를 때
 * <b>어디가</b> 다른지는 모르므로, 다르면 전체 재구축이 답이 된다.
 *
 * <hr>
 *
 * <p><b>계산 규칙 — product-service 의 같은 이름 클래스와 반드시 일치해야 한다.</b>
 *
 * <ol>
 *   <li>{@code productId} 오름차순으로 정렬한다.</li>
 *   <li>각 상품을 {@code id:name:price:stock:version} 으로 잇는다.</li>
 *   <li>{@code price} 는 {@code stripTrailingZeros().toPlainString()} 으로 정규화한다.</li>
 *   <li>상품마다 개행(\n)으로 잇고 UTF-8 로 SHA-256 을 구해 소문자 16진수로 만든다.</li>
 * </ol>
 *
 * <p>3번이 특히 중요하다. 같은 금액이라도 DB 에서 읽어 오면 {@code 89000} 이 되기도
 * {@code 89000.00} 이 되기도 한다. 정규화하지 않으면 <b>값이 같은데 체크섬만 달라</b>
 * 영원히 불일치로 나온다.
 *
 * <p>이 규칙이 두 서비스에 <b>코드가 아니라 문서로만</b> 묶여 있다는 것이 이 방식의
 * 대가다. 공통 모듈을 두지 않는 방침의 비용을 여기서 한 번 더 지불한다. 메시지
 * record 중복과 달리 여기서는 <b>필드 이름이 아니라 계산 로직</b>이 걸리므로 어긋나기
 * 더 쉽다. 그래서 양쪽 테스트에 <b>같은 입력의 기대 체크섬을 고정</b>해 두었다 —
 * 한쪽 규칙이 바뀌면 빌드가 깨진다.
 */
final class ProductChecksum {

    private ProductChecksum() {
    }

    static String of(List<ProductReplica> replicas) {
        StringBuilder sb = new StringBuilder();
        replicas.stream()
                .sorted((a, b) -> Long.compare(a.getProductId(), b.getProductId()))
                .forEach(r -> sb.append(line(r.getProductId(), r.getName(), r.getPrice(), r.getStock(),
                        r.getVersion())));
        return sha256(sb.toString());
    }

    static String line(Long id, String name, BigDecimal price, int stock, long version) {
        return id + ":" + name + ":" + normalize(price) + ":" + stock + ":" + version + "\n";
    }

    static String normalize(BigDecimal price) {
        return price == null ? "" : price.stripTrailingZeros().toPlainString();
    }

    static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 은 모든 JVM 이 반드시 제공한다. 여기에 오면 환경이 깨진 것이다.
            throw new IllegalStateException("SHA-256 을 쓸 수 없다", e);
        }
    }
}

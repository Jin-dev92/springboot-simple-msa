package com.example.msa.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * product-service 호출이 실패했을 때 대신 실행되는 코드.
 *
 * <p><b>모든 호출에 의미 있는 대체값이 있는 것은 아니다.</b> 예를 들어 "추천 상품 목록"이라면
 * 빈 목록을 돌려주고 화면을 그리는 것이 낫다. 하지만 여기서 필요한 것은 <b>가격</b>이고,
 * 가격을 지어낼 수는 없다. 0원으로 주문을 받으면 잘못된 데이터가 DB 에 남는다.
 *
 * <p>그래서 이 fallback 은 대체값을 만들지 않고 <b>503 으로 명확하게 거절</b>한다.
 * 그래도 얻는 것이 있다. fallback 이 없을 때는 연결 타임아웃만큼 기다린 끝에 500 이 났지만,
 * 회로가 열린 뒤에는 시도조차 하지 않고 즉시 503 을 돌려준다. 서킷 브레이커의 가치는
 * "그럴듯한 가짜 응답"이 아니라 <b>빠르고 정직한 실패</b>에 있다.
 *
 * <p>FallbackFactory 를 쓰면 실패 원인(cause)을 받을 수 있다. 로그를 보면 회로가 닫혀 있어
 * 실제로 시도했다가 실패한 것인지(ConnectException), 회로가 열려 있어 시도조차 하지 않은
 * 것인지(CallNotPermittedException) 구분된다.
 */
@Component
class ProductClientFallback implements FallbackFactory<ProductClient> {

    private static final Logger log = LoggerFactory.getLogger(ProductClientFallback.class);

    @Override
    public ProductClient create(Throwable cause) {
        return new ProductClient() {

            @Override
            public ProductResponse findById(Long id) {
                log.warn("product-service 조회 실패 → 주문 거절. 원인: {}", cause.toString());
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                        "상품 정보를 조회할 수 없어 주문을 받을 수 없습니다");
            }

            /**
             * 여기서는 <b>던지지 않는다.</b> 위 findById 와 정반대 선택이다.
             *
             * <p>가격은 지어낼 수 없으니 주문을 거절해야 하지만, 체크섬 조회 실패는
             * "복제본이 틀렸다"가 아니라 <b>"지금은 확인할 수 없다"</b>일 뿐이다.
             * 확인 못 한 것을 불일치로 세면 상대가 잠깐 죽을 때마다 거짓 경보가 뜬다.
             * {@code null} 을 돌려 검증기가 이번 회차를 건너뛰게 한다.
             */
            @Override
            public ChecksumResponse checksum() {
                log.debug("원본 체크섬 조회 실패. 이번 검증은 건너뛴다. 원인: {}", cause.toString());
                return null;
            }
        };
    }
}

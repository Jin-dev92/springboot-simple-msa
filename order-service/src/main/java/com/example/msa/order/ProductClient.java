package com.example.msa.order;

import java.math.BigDecimal;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * product-service 를 호출하는 HTTP 클라이언트.
 *
 * <p>주목할 점은 IP 도 포트도 적지 않는다는 것이다. {@code name = "product-service"} 는
 * Eureka 에 등록된 서비스 이름이며, 실제 주소는 호출 시점에 레지스트리에서 조회해 채워진다.
 * 인스턴스가 여러 개면 그 중 하나를 골라 주는 로드밸런싱도 여기서 일어난다.
 */
@FeignClient(name = "product-service", fallbackFactory = ProductClientFallback.class)
public interface ProductClient {

    @GetMapping("/products/{id}")
    ProductResponse findById(@PathVariable("id") Long id);

    /**
     * product-service 의 Product 와 필드가 겹치지만 일부러 공유하지 않는다.
     * 공유 모듈로 묶는 순간 두 서비스는 함께 배포해야 하는 하나의 덩어리가 된다.
     * 이 중복은 독립 배포를 얻기 위해 지불하는 의도된 비용이다.
     */
    record ProductResponse(Long id, String name, BigDecimal price, int stock) {
    }
}

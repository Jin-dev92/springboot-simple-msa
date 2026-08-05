package com.example.msa.product;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * 게이트웨이는 외부의 {@code /api/products/**} 를 {@code /products/**} 로 바꿔 전달한다.
 * order-service 도 Feign 으로 같은 경로를 호출한다.
 */
@RestController
@RequestMapping("/products")
class ProductController {

    // 인스턴스를 여러 개 띄웠을 때 어느 쪽이 요청을 받았는지 보기 위한 로그.
    // 인스턴스 구분은 별도로 심을 필요 없이 Compose 가 붙여 주는 컨테이너 이름 접두사
    // (product-service-1 | ... / product-service-2 | ...)로 확인한다.
    private static final Logger log = LoggerFactory.getLogger(ProductController.class);

    private final ProductRepository repository;

    ProductController(ProductRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    List<Product> findAll() {
        return repository.findAll();
    }

    @GetMapping("/{id}")
    Product findById(@PathVariable Long id) {
        log.info("상품 조회 요청: id={}", id);
        return repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "상품을 찾을 수 없습니다: " + id));
    }
}

package com.example.msa.product;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.transaction.annotation.Transactional;
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
    private final ProductEventPublisher events;

    ProductController(ProductRepository repository, ProductEventPublisher events) {
        this.repository = repository;
        this.events = events;
    }

    @GetMapping
    List<Product> findAll() {
        return repository.findAll();
    }

    /**
     * 상품 등록. 접근 제어는 여기가 아니라 {@link SecurityConfig} 에 선언되어 있다
     * ({@code hasRole("ADMIN")}). 인증된 사용자라도 ROLE_ADMIN 이 아니면 403 이 난다.
     */
    // 저장과 이벤트 기록을 한 트랜잭션으로 묶는다. 둘이 갈라지면 구독자의 복제본에
    // 새 상품이 영영 나타나지 않는다.
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Transactional
    Product create(@Valid @RequestBody ProductRequest request) {
        log.info("상품 등록: name={}", request.name());
        Product saved = repository.save(new Product(request.name(), request.price(),
                request.stock()));
        events.productChanged(saved);
        return saved;
    }

    /**
     * 상품 전체의 요약값. 구독자가 자기 복제본과 대조하는 데 쓴다.
     *
     * <p>공개 경로로 둔다. 상품 목록 자체가 이미 공개이므로 그 요약값이 새로 흘리는
     * 정보가 없다. 대신 계산이 전체 조회를 동반하므로, 상품이 많아지면 캐시하거나
     * 증분으로 유지해야 한다.
     */
    @GetMapping("/checksum")
    ChecksumResponse checksum() {
        List<Product> all = repository.findAll();
        return new ChecksumResponse(all.size(), ProductChecksum.of(all));
    }

    record ChecksumResponse(int count, String checksum) {
    }

    @GetMapping("/{id}")
    Product findById(@PathVariable Long id) {
        log.info("상품 조회 요청: id={}", id);
        return repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "상품을 찾을 수 없습니다: " + id));
    }

    record ProductRequest(
            @NotBlank String name,
            @NotNull @Positive BigDecimal price,
            @PositiveOrZero int stock) {
    }
}

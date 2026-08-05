package com.example.msa.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
class OrderController {

    private final OrderRepository repository;
    private final ProductClient productClient;

    OrderController(OrderRepository repository, ProductClient productClient) {
        this.repository = repository;
        this.productClient = productClient;
    }

    @GetMapping
    List<Order> findAll() {
        return repository.findAll();
    }

    /**
     * 곱셈 한 번을 위해 서비스 경계를 한 번 넘는다. 이 호출이 이 프로젝트의 핵심 학습 지점이다.
     *
     * <p>가격은 product-service 만 알고 있으므로 order-service 가 임의로 가지고 있을 수 없다.
     * 상품이 없으면 Feign 이 404 를 예외로 던지고, product-service 가 죽어 있으면 호출 자체가
     * 실패한다. 이 취약함을 Phase 5 의 서킷 브레이커로 다루게 된다.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Order create(@Valid @RequestBody OrderRequest request) {
        ProductClient.ProductResponse product = productClient.findById(request.productId());
        BigDecimal totalPrice = product.price().multiply(BigDecimal.valueOf(request.quantity()));
        return repository.save(new Order(product.id(), request.quantity(), totalPrice));
    }

    // 외부 입력은 신뢰 경계이므로 값 검증을 건너뛰지 않는다.
    record OrderRequest(@NotNull Long productId, @Positive int quantity) {
    }
}

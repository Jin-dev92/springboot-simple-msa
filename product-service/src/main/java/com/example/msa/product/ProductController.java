package com.example.msa.product;

import java.util.List;
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
        return repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "상품을 찾을 수 없습니다: " + id));
    }
}

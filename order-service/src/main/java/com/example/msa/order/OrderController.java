package com.example.msa.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String, Object> kafkaTemplate;

    OrderController(OrderRepository repository, ProductClient productClient,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.repository = repository;
        this.productClient = productClient;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    List<Order> findAll() {
        return repository.findAll();
    }

    /**
     * 한 번의 주문 생성에 성격이 다른 두 종류의 통신이 들어 있다.
     *
     * <p><b>가격 조회는 동기</b>다. 총액을 계산해 응답에 담아야 하므로 답을 기다릴 수밖에 없다.
     * 대가로 product-service 가 죽어 있으면 주문도 함께 실패한다. 이 취약함을 Phase 5 의
     * 서킷 브레이커로 다룬다.
     *
     * <p><b>재고 차감은 비동기</b>다. 주문 응답에 재고를 담을 필요가 없으므로 Kafka 에
     * 이벤트만 던지고 바로 응답한다. product-service 가 이벤트를 언제 처리하든 주문은 이미
     * 성공한 상태다. 대신 "주문 직후 재고를 조회하면 아직 안 깎여 있을 수 있다"는 것을
     * 받아들여야 한다. 이를 결과적 일관성(eventual consistency)이라고 한다.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Order create(@Valid @RequestBody OrderRequest request) {
        ProductClient.ProductResponse product = productClient.findById(request.productId());
        BigDecimal totalPrice = product.price().multiply(BigDecimal.valueOf(request.quantity()));
        Order order = repository.save(new Order(product.id(), request.quantity(), totalPrice));

        kafkaTemplate.send(OrderCreatedEvent.TOPIC,
                new OrderCreatedEvent(order.getId(), order.getProductId(), order.getQuantity()));
        return order;
    }

    // 외부 입력은 신뢰 경계이므로 값 검증을 건너뛰지 않는다.
    record OrderRequest(@NotNull Long productId, @Positive int quantity) {
    }
}

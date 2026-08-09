package com.example.msa.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/orders")
class OrderController {

    private final OrderRepository repository;
    private final ProductClient productClient;
    private final OrderSagaOrchestrator orchestrator;

    OrderController(OrderRepository repository, ProductClient productClient,
            OrderSagaOrchestrator orchestrator) {
        this.repository = repository;
        this.productClient = productClient;
        this.orchestrator = orchestrator;
    }

    /**
     * 본인 주문만 돌려준다.
     *
     * <p>이 메서드가 이 프로젝트에서 가장 중요한 보안 지점이다. 역할 검사(ROLE_USER 인가)는
     * <b>무엇을 할 수 있는가</b>만 판단한다. <b>누구의 데이터인가</b>는 전혀 보지 않는다.
     * 둘을 구분하지 않으면 로그인한 사용자 누구나 남의 주문을 조회할 수 있게 된다.
     * 이런 결함을 흔히 IDOR(Insecure Direct Object Reference)라고 부른다.
     *
     * <p>사용자 id 를 요청 파라미터로 받지 않는다는 점도 중요하다. 클라이언트가 보낸 값을
     * 믿으면 {@code ?userId=2} 로 바꿔 남의 주문을 볼 수 있다. 반드시 <b>서명이 검증된
     * 토큰</b>에서 꺼내야 한다.
     */
    @GetMapping
    List<Order> findMyOrders(@AuthenticationPrincipal Jwt jwt) {
        return repository.findByUserId(userIdOf(jwt));
    }

    @GetMapping("/{id}")
    Order findMyOrder(@PathVariable Long id, @AuthenticationPrincipal Jwt jwt) {
        return repository.findByIdAndUserId(id, userIdOf(jwt))
                // 남의 주문이면 403 이 아니라 404 를 준다. 403 은 "그 주문은 존재하지만
                // 네 것이 아니다"라는 정보를 흘려 주기 때문이다.
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "주문을 찾을 수 없습니다: " + id));
    }

    /** JWT 의 uid 클레임을 꺼낸다. JSON 숫자는 Integer 또는 Long 으로 오므로 Number 로 받는다. */
    private static Long userIdOf(Jwt jwt) {
        return ((Number) jwt.getClaim("uid")).longValue();
    }

    /**
     * 한 번의 주문 생성에 성격이 다른 두 종류의 통신이 들어 있다.
     *
     * <p><b>가격 조회는 동기</b>다. 총액을 계산해 응답에 담아야 하므로 답을 기다릴 수밖에 없다.
     * 대가로 product-service 가 죽어 있으면 주문도 함께 실패한다. 이 취약함을 Phase 5 의
     * 서킷 브레이커로 다룬다.
     *
     * <p><b>재고 차감은 비동기</b>다. 주문 응답에 재고를 담을 필요가 없으므로 Kafka 에
     * 이벤트만 던지고 바로 응답한다. 대신 "주문 직후 재고를 조회하면 아직 안 깎여 있을 수
     * 있다"는 것을 받아들여야 한다. 이를 결과적 일관성(eventual consistency)이라고 한다.
     *
     * <p>그래서 이 시점의 주문은 <b>PENDING</b> 이다. 재고와 결제가 끝났는지 아직 모르기
     * 때문이다. 이후 진행은 {@link OrderSagaOrchestrator} 가 맡는다. 컨트롤러는 어떤
     * 메시지를 어느 토픽으로 보내는지 알 필요가 없다. 201 이 "주문이 확정됐다"가 아니라
     * "주문 요청을 접수했다"라는 뜻이 된다는 점이 나누기 전과 달라진 부분이다.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Order create(@Valid @RequestBody OrderRequest request, @AuthenticationPrincipal Jwt jwt) {
        ProductClient.ProductResponse product = productClient.findById(request.productId());
        BigDecimal totalPrice = product.price().multiply(BigDecimal.valueOf(request.quantity()));
        Order order = repository.save(
                new Order(userIdOf(jwt), product.id(), product.name(), request.quantity(),
                        totalPrice));

        orchestrator.start(order);
        return order;
    }

    // 외부 입력은 신뢰 경계이므로 값 검증을 건너뛰지 않는다.
    record OrderRequest(@NotNull Long productId, @Positive int quantity) {
    }
}

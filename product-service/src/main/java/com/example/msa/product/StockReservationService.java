package com.example.msa.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 명령 처리. Saga 에서 이 서비스가 맡은 로컬 트랜잭션이다.
 *
 * <p>리스너와 별도의 빈으로 둔 데는 이유가 있다. {@code @Transactional} 은 스프링이
 * 만든 프록시를 거쳐야 동작하는데, <b>같은 클래스 안에서 자기 메서드를 호출하면
 * 프록시를 거치지 않아 트랜잭션이 걸리지 않는다</b>(self-invocation). 조용히 실패하기
 * 때문에 알아채기 어렵다. 트랜잭션 경계를 다른 빈으로 옮기면 이 문제가 사라진다.
 *
 * <p>이 클래스에는 "다음에 무엇을 할지"에 대한 판단이 없다. 재고를 잡거나 되돌리고
 * 결과를 돌려줄 뿐이다. 흐름은 order-service 의 오케스트레이터가 정한다.
 */
@Service
class StockReservationService {

    private static final Logger log = LoggerFactory.getLogger(StockReservationService.class);

    private final ProductRepository repository;
    private final ProcessedCommandRepository processed;

    StockReservationService(ProductRepository repository, ProcessedCommandRepository processed) {
        this.repository = repository;
        this.processed = processed;
    }

    /**
     * 재고 변경과 처리 기록을 <b>하나의 트랜잭션</b>으로 묶는다.
     *
     * <p>따로 커밋하면 "재고는 깎였는데 기록은 없는" 상태가 생기고, 그러면 같은
     * 명령이 다시 왔을 때 또 깎인다. 둘이 같은 DB 안에 있으므로 묶을 수 있다.
     */
    // public 인 이유: @Transactional 이 non-public 메서드에도 걸리는지는 스프링 버전과
    // 프록시 방식에 따라 달라진다. 조용히 무시되면 알아채기 어려우므로 확실한 쪽을 택했다.
    // 클래스 자체가 package-private 이라 실제 노출 범위는 그대로다.
    @Transactional
    public SagaReply handle(StockCommand command) {
        // 이미 처리한 명령이면 재고를 다시 건드리지 않고 그때의 결론을 그대로 돌려준다.
        // 그냥 건너뛰고 아무것도 돌려주지 않으면, 첫 처리에서 응답 발행이 실패했을 경우
        // Saga 가 영원히 대기 상태로 남는다.
        var seen = processed.findById(ProcessedCommand.idOf(command.orderId(), command.action()));
        if (seen.isPresent()) {
            ProcessedCommand record = seen.get();
            log.info("이미 처리한 명령. 재고는 건드리지 않고 결과만 재발행: orderId={}, action={}, 최초처리={}",
                    command.orderId(), command.action(), record.getProcessedAt());
            return new SagaReply(command.orderId(), command.action().name(),
                    record.isSuccess(), record.getReason());
        }

        SagaReply reply = repository.findById(command.productId())
                .map(product -> apply(product, command))
                .orElseGet(() -> {
                    log.warn("존재하지 않는 상품에 대한 명령: productId={}", command.productId());
                    return SagaReply.fail(command.orderId(), command.action(),
                            "존재하지 않는 상품입니다");
                });

        processed.save(new ProcessedCommand(command.orderId(), command.action(),
                reply.success(), reply.reason()));
        return reply;
    }

    private SagaReply apply(Product product, StockCommand command) {
        return switch (command.action()) {
            case RESERVE -> reserve(product, command);
            case RELEASE -> release(product, command);
        };
    }

    private SagaReply reserve(Product product, StockCommand command) {
        if (!product.decreaseStock(command.quantity())) {
            log.warn("재고 부족: productId={}, 주문수량={}, 현재재고={} (orderId={})",
                    command.productId(), command.quantity(), product.getStock(), command.orderId());
            return SagaReply.fail(command.orderId(), command.action(),
                    "재고 부족 (요청 %d, 남은 재고 %d)"
                            .formatted(command.quantity(), product.getStock()));
        }

        repository.save(product);
        log.info("재고 차감: productId={}, 주문수량={}, 남은재고={} (orderId={})",
                command.productId(), command.quantity(), product.getStock(), command.orderId());
        return SagaReply.ok(command.orderId(), command.action());
    }

    /**
     * 보상. 차감했던 재고를 되돌린다.
     *
     * <p>실패할 수 있는 조건이 없다는 점이 실행과 다르다. 되돌리는 수량은 앞서
     * 실제로 차감한 수량이기 때문이다. <b>보상은 실패하면 안 되는 연산</b>이며,
     * 그래서 보상 단계를 설계할 때는 검증이 필요 없는 형태로 만드는 것이 좋다.
     */
    private SagaReply release(Product product, StockCommand command) {
        product.increaseStock(command.quantity());
        repository.save(product);
        log.info("재고 복구(보상): productId={}, 복구수량={}, 현재재고={} (orderId={})",
                command.productId(), command.quantity(), product.getStock(), command.orderId());
        return SagaReply.ok(command.orderId(), command.action());
    }
}

package com.example.msa.product;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 재고 확보 판단. Saga 에서 이 서비스가 맡은 로컬 트랜잭션이다.
 *
 * <p>리스너와 별도의 빈으로 둔 데는 이유가 있다. {@code @Transactional} 은 스프링이
 * 만든 프록시를 거쳐야 동작하는데, <b>같은 클래스 안에서 자기 메서드를 호출하면
 * 프록시를 거치지 않아 트랜잭션이 걸리지 않는다</b>(self-invocation). 조용히 실패하기
 * 때문에 알아채기 어렵다. 트랜잭션 경계를 다른 빈으로 옮기면 이 문제가 사라진다.
 */
@Service
class StockReservationService {

    private static final Logger log = LoggerFactory.getLogger(StockReservationService.class);

    private final ProductRepository repository;
    private final ProcessedOrderEventRepository processedEvents;

    StockReservationService(ProductRepository repository,
            ProcessedOrderEventRepository processedEvents) {
        this.repository = repository;
        this.processedEvents = processedEvents;
    }

    /**
     * 재고 차감과 처리 기록을 <b>하나의 트랜잭션</b>으로 묶는다.
     *
     * <p>따로 커밋하면 "재고는 깎였는데 기록은 없는" 상태가 생기고, 그러면 같은
     * 이벤트가 다시 왔을 때 또 깎인다. 둘이 같은 DB 안에 있으므로 묶을 수 있다.
     */
    // public 인 이유: @Transactional 이 non-public 메서드에도 걸리는지는 스프링 버전과
    // 프록시 방식에 따라 달라진다. 조용히 무시되면 알아채기 어려우므로 확실한 쪽을 택했다.
    // 클래스 자체가 package-private 이라 실제 노출 범위는 그대로다.
    @Transactional
    public StockResultEvent reserve(OrderCreatedEvent event) {
        // 이미 처리한 주문이면 재고를 다시 건드리지 않고 그때의 결론을 그대로 돌려준다.
        // 그냥 건너뛰고 아무것도 돌려주지 않으면, 첫 처리에서 결과 발행이 실패했을 경우
        // 주문이 PENDING 으로 영원히 남는다.
        var seen = processedEvents.findById(event.orderId());
        if (seen.isPresent()) {
            ProcessedOrderEvent record = seen.get();
            log.info("이미 처리한 주문. 재고는 건드리지 않고 결과만 재발행: orderId={}, 최초처리={}",
                    event.orderId(), record.getProcessedAt());
            return new StockResultEvent(event.orderId(), event.productId(), event.quantity(),
                    record.isReserved(), record.getReason());
        }

        StockResultEvent result = repository.findById(event.productId())
                .map(product -> decrease(product, event))
                .orElseGet(() -> {
                    log.warn("존재하지 않는 상품에 대한 주문 이벤트: productId={}", event.productId());
                    return StockResultEvent.rejected(event.orderId(), event.productId(),
                            event.quantity(), "존재하지 않는 상품입니다");
                });

        processedEvents.save(
                new ProcessedOrderEvent(event.orderId(), result.reserved(), result.reason()));
        return result;
    }

    private StockResultEvent decrease(Product product, OrderCreatedEvent event) {
        if (!product.decreaseStock(event.quantity())) {
            log.warn("재고 부족: productId={}, 주문수량={}, 현재재고={} (orderId={})",
                    event.productId(), event.quantity(), product.getStock(), event.orderId());
            return StockResultEvent.rejected(event.orderId(), event.productId(), event.quantity(),
                    "재고 부족 (요청 %d, 남은 재고 %d)".formatted(event.quantity(), product.getStock()));
        }

        repository.save(product);
        log.info("재고 차감: productId={}, 주문수량={}, 남은재고={} (orderId={})",
                event.productId(), event.quantity(), product.getStock(), event.orderId());
        return StockResultEvent.reserved(event.orderId(), event.productId(), event.quantity());
    }
}

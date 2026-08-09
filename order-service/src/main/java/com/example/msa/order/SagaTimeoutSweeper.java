package com.example.msa.order;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 응답이 오지 않은 채 멈춰 있는 Saga 를 찾아 보상을 개시한다.
 *
 * <p><b>코레오그래피에서는 만들 수 없던 기능이다.</b> 그때는 주문이 PENDING 에서
 * 멈춰 있어도 요청이 도달하지 못한 것인지 응답이 유실된 것인지 판단할 근거가
 * 시스템 어디에도 없었다. 진행 상태를 한 곳에 모았기 때문에 비로소
 * "어디서 멈췄는가"를 질의할 수 있게 되었다.
 *
 * <p>이 클래스는 찾기만 하고 처리는 오케스트레이터에 넘긴다. 흐름 판단을 한 곳에
 * 두기 위해서이기도 하고, Saga 마다 별도 트랜잭션으로 돌리기 위해서이기도 하다.
 * 다른 빈의 {@code @Transactional} 메서드를 부르면 프록시를 거치므로 호출 하나가
 * 트랜잭션 하나가 되고, 하나가 실패해도 나머지가 함께 롤백되지 않는다.
 */
@Component
class SagaTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(SagaTimeoutSweeper.class);

    private final OrderSagaRepository sagas;
    private final OrderSagaOrchestrator orchestrator;
    private final Duration timeout;

    SagaTimeoutSweeper(OrderSagaRepository sagas, OrderSagaOrchestrator orchestrator,
            @Value("${saga.timeout.threshold}") Duration timeout) {
        this.sagas = sagas;
        this.orchestrator = orchestrator;
        this.timeout = timeout;
    }

    /**
     * {@code fixedDelay} 는 <b>이전 실행이 끝난 뒤부터</b> 간격을 잰다. 처리가 오래
     * 걸려도 다음 실행이 겹쳐 들어오지 않는다. {@code fixedRate} 였다면 겹칠 수 있다.
     */
    @Scheduled(fixedDelayString = "${saga.timeout.check-interval}")
    void sweep() {
        Instant deadline = Instant.now().minus(timeout);
        List<OrderSaga> stalled =
                sagas.findByStepInAndUpdatedAtBefore(SagaStep.waiting(), deadline);
        if (stalled.isEmpty()) {
            return;
        }

        log.warn("응답이 없는 사가 {}건을 처리한다 (임계 {})", stalled.size(), timeout);
        // 조회 결과를 그대로 들고 반복한다. 각 호출이 별도 트랜잭션이므로
        // 오케스트레이터가 상태를 다시 읽어 확인한다.
        stalled.forEach(saga -> orchestrator.onTimeout(saga.getOrderId()));
    }
}

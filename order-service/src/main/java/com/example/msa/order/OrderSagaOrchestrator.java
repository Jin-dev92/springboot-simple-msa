package com.example.msa.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 주문 Saga 의 조정자. <b>흐름 전체가 이 한 클래스에 있다.</b>
 *
 * <p>코레오그래피에서는 흐름이 order-service 와 product-service 의 리스너에 나뉘어
 * 있었다. 전체 순서를 알려면 두 저장소의 파일을 읽고 머릿속에서 이어 붙여야 했고,
 * 참여자가 늘수록 그만큼 더 흩어졌다. 오케스트레이션은 그 결정 로직을 한 곳으로
 * 모은다. 참여자는 시키는 일을 하고 답할 뿐, 자기 앞뒤에 무엇이 있는지 모른다.
 *
 * <p>대가도 있다. 이 클래스가 참여자 전부를 알아야 하므로, 단계를 추가하면 참여자
 * 코드는 그대로여도 여기는 반드시 바뀐다. 코레오그래피에서는 새 참여자가 이벤트를
 * 구독하기만 하면 됐다. <b>어디를 고치게 될 것인가의 문제</b>이지 한쪽이 늘 나은
 * 것은 아니다.
 */
@Component
class OrderSagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaOrchestrator.class);

    private final OrderRepository orders;
    private final OrderSagaRepository sagas;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    OrderSagaOrchestrator(OrderRepository orders, OrderSagaRepository sagas,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.orders = orders;
        this.sagas = sagas;
        this.kafkaTemplate = kafkaTemplate;
    }

    /** 주문이 저장된 직후 호출된다. 첫 단계인 재고 확보를 시작한다. */
    @Transactional
    void start(Order order) {
        sagas.save(new OrderSaga(order.getId()));
        sendStock(order, StockCommand.Action.RESERVE);
    }

    /**
     * 참여자의 응답을 받아 다음 단계를 정한다.
     *
     * <p>가장 먼저 하는 일이 <b>기다리던 응답이 맞는지 확인하는 것</b>이다. 타임아웃으로
     * 이미 보상에 들어갔거나 끝난 Saga 에 늦은 응답이 도착할 수 있고, 그것을 그대로
     * 반영하면 취소된 주문이 확정으로 되살아난다.
     */
    @Transactional
    void onReply(SagaReply reply) {
        OrderSaga saga = sagas.findById(reply.orderId()).orElse(null);
        if (saga == null) {
            log.warn("모르는 주문의 응답을 받았다: orderId={}, action={}",
                    reply.orderId(), reply.action());
            return;
        }
        if (!saga.getStep().expects(reply.action())) {
            log.warn("기다리는 단계와 어긋난 응답이라 버린다: orderId={}, 현재단계={}, 응답={}",
                    reply.orderId(), saga.getStep(), reply.action());
            return;
        }

        switch (saga.getStep()) {
            case RESERVING_STOCK -> afterReserve(saga, reply);
            case CHARGING_PAYMENT -> afterCharge(saga, reply);
            case COMPENSATING_STOCK -> afterRelease(saga, reply);
            default -> throw new IllegalStateException(
                    "응답을 기다리지 않는 단계인데 expects 를 통과했다: " + saga.getStep());
        }
    }

    /**
     * 응답이 오지 않은 채 시간이 지난 Saga 를 처리한다. {@link SagaTimeoutSweeper} 가 호출한다.
     *
     * <p>Saga 마다 별도 트랜잭션으로 돌리기 위해 스위퍼와 다른 빈에 두었다.
     * 하나가 실패해도 나머지 처리가 함께 롤백되지 않는다.
     */
    @Transactional
    void onTimeout(Long orderId) {
        OrderSaga saga = sagas.findById(orderId).orElse(null);
        if (saga == null) {
            return;
        }

        switch (saga.getStep()) {
            // 되돌릴 앞 단계가 없다. 바로 끝낸다.
            case RESERVING_STOCK -> finish(saga, "재고 확보 응답이 없어 주문을 취소했습니다");
            // 재고는 이미 잡혀 있을 수 있으므로 반드시 되돌려야 한다.
            case CHARGING_PAYMENT -> compensate(saga, "결제 응답이 없어 주문을 취소했습니다");
            // 보상 명령이 유실됐을 수 있다. 다시 보낸다.
            case COMPENSATING_STOCK -> {
                log.warn("보상 응답이 없어 재고 복구 명령을 다시 보낸다: orderId={}", orderId);
                saga.touch();
                sagas.save(saga);
                orders.findById(orderId)
                        .ifPresent(order -> sendStock(order, StockCommand.Action.RELEASE));
            }
            default -> log.debug("이미 끝난 사가라 타임아웃 처리가 필요 없다: orderId={}", orderId);
        }
    }

    private void afterReserve(OrderSaga saga, SagaReply reply) {
        if (!reply.success()) {
            // 재고를 못 잡았다면 되돌릴 앞 단계가 없다. 주문 상태만 정리하면 끝이다.
            finish(saga, reply.reason());
            return;
        }

        saga.moveTo(SagaStep.CHARGING_PAYMENT);
        sagas.save(saga);

        orders.findById(saga.getOrderId()).ifPresent(order ->
                kafkaTemplate.send(PaymentCommand.TOPIC, key(order.getId()),
                        new PaymentCommand(order.getId(), order.getUserId(),
                                order.getTotalPrice(), PaymentCommand.Action.CHARGE)));
        log.info("재고 확보 완료. 결제를 요청한다: orderId={}", saga.getOrderId());
    }

    private void afterCharge(OrderSaga saga, SagaReply reply) {
        if (reply.success()) {
            saga.moveTo(SagaStep.COMPLETED);
            sagas.save(saga);
            orders.findById(saga.getOrderId()).ifPresent(order -> {
                order.confirm();
                orders.save(order);
            });
            log.info("주문 확정: orderId={}", saga.getOrderId());
            return;
        }

        compensate(saga, reply.reason());
    }

    private void afterRelease(OrderSaga saga, SagaReply reply) {
        if (!reply.success()) {
            // 보상 실패는 자동으로 풀 수 없다. 재고를 되돌리지 못했는데 또 무엇을
            // 되돌린다는 것이 성립하지 않기 때문이다. 사람이 개입해야 하는 지점이다.
            // 실무에서는 여기에 DLQ 와 운영 알림이 붙는다.
            log.error("보상에 실패했다. 재고가 묶인 채 남아 있을 수 있으니 확인이 필요하다: orderId={}, 사유={}",
                    saga.getOrderId(), reply.reason());
        }
        finish(saga, saga.getFailReason());
    }

    /** 앞 단계(재고)를 되돌리도록 지시한다. 최종 취소는 그 응답을 받은 뒤에 한다. */
    private void compensate(OrderSaga saga, String reason) {
        saga.moveTo(SagaStep.COMPENSATING_STOCK, reason);
        sagas.save(saga);

        orders.findById(saga.getOrderId())
                .ifPresent(order -> sendStock(order, StockCommand.Action.RELEASE));
        log.warn("보상 개시. 재고 복구를 요청한다: orderId={}, 사유={}", saga.getOrderId(), reason);
    }

    /**
     * Saga 를 실패로 끝내고 주문을 취소한다.
     *
     * <p>주문을 삭제하지 않고 CANCELLED 로 남긴다. 왜 취소됐는지가 사용자에게도
     * 운영자에게도 필요한 정보이기 때문이다. 보상은 "없던 일로 만들기"가 아니라
     * <b>되돌리는 효과를 내는 새로운 작업</b>이다.
     */
    private void finish(OrderSaga saga, String reason) {
        saga.moveTo(SagaStep.FAILED, reason);
        sagas.save(saga);

        orders.findById(saga.getOrderId()).ifPresent(order -> {
            order.cancel(reason);
            orders.save(order);
        });
        log.warn("주문 취소: orderId={}, 사유={}", saga.getOrderId(), reason);
    }

    private void sendStock(Order order, StockCommand.Action action) {
        kafkaTemplate.send(StockCommand.TOPIC, key(order.getId()),
                new StockCommand(order.getId(), order.getProductId(), order.getQuantity(), action));
    }

    /**
     * 메시지 키. 같은 주문의 명령이 같은 파티션에 들어가야 실행과 보상의 순서가
     * 지켜진다. 키가 없으면 라운드로빈으로 흩어져 순서 보장이 사라진다.
     */
    private static String key(Long orderId) {
        return String.valueOf(orderId);
    }
}

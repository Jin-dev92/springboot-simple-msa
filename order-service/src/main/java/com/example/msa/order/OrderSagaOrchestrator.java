package com.example.msa.order;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

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
    // public 인 이유: @Transactional 이 non-public 메서드에도 걸리는지는 스프링 버전과
    // 프록시 방식에 따라 달라진다. 조용히 무시되면 알아채기 어려우므로 확실한 쪽을 택했다.
    // 클래스 자체가 package-private 이라 실제 노출 범위는 그대로다.
    @Transactional
    public void start(Order order) {
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
    // public 인 이유는 start 위의 주석과 같다.
    @Transactional
    public void onReply(SagaReply reply) {
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
            // 위의 expects 검사를 통과했다면 이 분기는 도달할 수 없다. 그래도 던지지 않고
            // 로그만 남기는 이유: 이 메서드는 카프카 리스너 아래에서 호출된다. 예외를
            // 던지면 스프링의 기본 에러 핸들러가 정해진 횟수만 재시도하고(무한이 아니다)
            // 그래도 실패하면 조용히 오프셋을 넘긴다 — 응답이 그냥 사라진다. 로그만
            // 남기고 넘어가면 그 응답은 버려지되 사가는 살아 있어, 나중에 스위퍼가
            // 다시 집어 갈 수 있다. 이 쪽이 진짜 이유다.
            default -> log.error("응답을 기다리지 않는 단계인데 expects 를 통과했다: orderId={}, 단계={}",
                    saga.getOrderId(), saga.getStep());
        }
    }

    /**
     * 응답이 오지 않은 채 시간이 지난 Saga 를 처리한다. {@link SagaTimeoutSweeper} 가 호출한다.
     *
     * <p>Saga 마다 별도 트랜잭션으로 돌리기 위해 스위퍼와 다른 빈에 두었다.
     * 하나가 실패해도 나머지 처리가 함께 롤백되지 않는다.
     */
    // public 인 이유는 start 위의 주석과 같다.
    @Transactional
    public void onTimeout(Long orderId) {
        OrderSaga saga = sagas.findById(orderId).orElse(null);
        if (saga == null) {
            return;
        }

        switch (saga.getStep()) {
            // 타임아웃만으로는 "확보가 실패했다"와 "확보는 됐는데 응답만 유실됐다"를
            // 구분할 수 없다. 그렇다고 무작정 RELEASE 를 보내면 release() 는 무조건
            // 복구하므로, 실제로는 없던 예약을 있던 것처럼 취급해 재고가 부풀어난다.
            // 두 선택 다 안전하지 않아, 재고가 묶일 위험 쪽을 택하고 주문을 취소한다.
            // 근본적으로는 참여자에게 그 예약이 실제로 됐는지 되물어야 하는 문제다.
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
                sendAfterCommit(PaymentCommand.TOPIC, key(order.getId()),
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
        sendAfterCommit(StockCommand.TOPIC, key(order.getId()),
                new StockCommand(order.getId(), order.getProductId(), order.getQuantity(), action));
    }

    /**
     * 메시지 키. 같은 주문의 명령이 같은 파티션에 들어가야 실행과 보상의 순서가
     * 지켜진다. 키가 없으면 라운드로빈으로 흩어져 순서 보장이 사라진다.
     */
    private static String key(Long orderId) {
        return String.valueOf(orderId);
    }

    /**
     * 트랜잭션이 커밋된 뒤에 발행한다.
     *
     * <p>트랜잭션 안에서 보내면 <b>커밋이 롤백돼도 명령은 이미 나가 있습니다.</b>
     * 스위퍼와 응답 리스너가 같은 사가를 동시에 옮겨 낙관적 락으로 한쪽이 롤백되는
     * 경우가 그렇다. DB 는 되돌아가지만 참여자는 이미 받은 명령을 수행하므로,
     * 주문은 확정인데 재고는 반납된 상태가 남는다. {@code @Version} 은 행을 지키지
     * 이미 나간 메시지를 지키지는 못한다.
     *
     * <p>반대 방향의 위험은 남는다. 커밋 뒤 발행 직전에 죽으면 명령이 나가지 않는다.
     * 다만 그때는 사가가 대기 단계에 그대로 머물러 스위퍼가 다시 집어 간다. 둘 중
     * 되돌릴 수 없는 쪽을 피한 선택이다. 완전한 해결은 Transactional Outbox 다.
     */
    private void sendAfterCommit(String topic, String key, Object payload) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            kafkaTemplate.send(topic, key, payload);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(topic, key, payload);
            }
        });
    }
}

package com.example.msa.order;

import java.util.List;

/**
 * Saga 의 진행 단계.
 *
 * <p>각 단계가 <b>어떤 응답을 기다리는지</b>를 함께 들고 있는 것이 요점이다. 응답이
 * 도착하면 "지금 기다리던 것이 맞는가"를 이 값으로 판단해, 어긋나는 응답을 버린다.
 * 이 검사가 없으면 타임아웃으로 이미 끝난 Saga 가 뒤늦게 도착한 성공 응답 때문에
 * 되살아난다.
 *
 * <p>완료한 단계의 목록을 따로 저장하지 않는 이유는 단계가 <b>선형</b>이기 때문이다.
 * "지금 어느 단계인가" 하나만 알면 되돌릴 대상이 결정된다. 분기하거나 병렬로
 * 갈라지는 Saga 라면 완료 목록이 필요해지지만, 그때 도입하면 된다.
 */
enum SagaStep {

    RESERVING_STOCK("RESERVE"),
    CHARGING_PAYMENT("CHARGE"),
    COMPENSATING_STOCK("RELEASE"),

    /** 모든 단계 성공. 더 기다릴 응답이 없다. */
    COMPLETED(null),

    /** 보상까지 끝난 최종 실패. 더 기다릴 응답이 없다. */
    FAILED(null);

    private final String expectedReply;

    SagaStep(String expectedReply) {
        this.expectedReply = expectedReply;
    }

    boolean expects(String action) {
        return expectedReply != null && expectedReply.equals(action);
    }

    /** 응답을 기다리는 중이라 타임아웃 대상이 되는 단계들. */
    static List<SagaStep> waiting() {
        return List.of(RESERVING_STOCK, CHARGING_PAYMENT, COMPENSATING_STOCK);
    }
}

package com.example.msa.order;

/**
 * 참여자들이 돌려주는 응답. 모든 참여자가 이 토픽 하나로 답한다.
 *
 * <p>참여자마다 응답 토픽을 따로 두지 않는 이유는, 받는 쪽이 오케스트레이터 하나뿐이고
 * 처리도 한 곳에서 하기 때문이다. 토픽을 늘리면 리스너만 늘어난다.
 *
 * <p>{@code action} 이 열거형이 아니라 문자열인 것은 이 필드에 서로 다른 서비스의
 * 열거형 값(RESERVE/RELEASE/CHARGE)이 모두 실리기 때문이다. 열거형으로 만들려면
 * 세 값을 다 아는 공통 타입이 필요해져 공통 모듈을 두지 않는 방침이 깨진다.
 * 받는 쪽은 {@link SagaStep#expects(String)} 로 문자열 비교만 하면 되므로 충분하다.
 */
public record SagaReply(Long orderId, String action, boolean success, String reason) {

    static final String TOPIC = "saga-reply";
}

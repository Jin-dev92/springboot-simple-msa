package com.example.msa.order;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import java.time.Duration;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 서킷 브레이커 설정.
 *
 * <p>서킷 브레이커는 두꺼비집과 같다. 상대 서비스가 계속 실패하면 회로를 열어(OPEN)
 * 아예 호출을 시도조차 하지 않는다. 이유는 두 가지다.
 *
 * <ol>
 *   <li><b>빠른 실패</b> — 어차피 실패할 호출에 타임아웃만큼 기다리지 않는다.
 *       그 대기 시간 동안 스레드가 묶이면 우리 서비스까지 함께 느려진다.</li>
 *   <li><b>회복할 틈 주기</b> — 이미 힘든 상대에게 재시도를 퍼붓지 않는다.</li>
 * </ol>
 *
 * <p>값은 학습용으로 작게 잡았다. 실제로는 트래픽 양에 맞춰 조정해야 하며,
 * 창(window)이 너무 작으면 우연한 실패 몇 번에 회로가 열린다.
 */
@Configuration
class ResilienceConfig {

    @Bean
    Customizer<Resilience4JCircuitBreakerFactory> circuitBreakerCustomizer() {
        return factory -> factory.configureDefault(id -> new Resilience4JConfigBuilder(id)
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        // 최근 호출 5건을 기준으로 실패율을 계산한다(시간이 아니라 횟수 기준).
                        .slidingWindowType(SlidingWindowType.COUNT_BASED)
                        .slidingWindowSize(5)
                        // 최소 3건은 모여야 판단한다. 첫 실패 한 번에 열리면 곤란하다.
                        .minimumNumberOfCalls(3)
                        // 실패율이 50%를 넘으면 회로를 연다(OPEN).
                        .failureRateThreshold(50.0f)
                        // OPEN 상태를 10초 유지한 뒤 HALF_OPEN 으로 넘어가 시험 호출을 보낸다.
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        // HALF_OPEN 에서 2건을 시험해 보고, 성공하면 CLOSED 로 돌아간다.
                        .permittedNumberOfCallsInHalfOpenState(2)
                        .build())
                // 타임아웃이 두 겹이라는 점에 주의. Feign 자체의 connect/read 타임아웃(2초)과
                // 별개로 Spring Cloud CircuitBreaker 가 TimeLimiter 를 씌우는데,
                // 그 기본값이 1초여서 명시하지 않으면 Feign 설정이 무의미해진다.
                // 여기서는 둘을 같은 값으로 맞춰 혼란을 없앤다.
                .timeLimiterConfig(TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(2))
                        .build())
                .build());
    }
}

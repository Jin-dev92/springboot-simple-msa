package com.example.msa.loadtest;

import static io.gatling.javaapi.core.CoreDsl.constantUsersPerSec;
import static io.gatling.javaapi.core.CoreDsl.details;
import static io.gatling.javaapi.core.CoreDsl.exec;
import static io.gatling.javaapi.core.CoreDsl.global;
import static io.gatling.javaapi.core.CoreDsl.listFeeder;
import static io.gatling.javaapi.core.CoreDsl.rampUsers;
import static io.gatling.javaapi.core.CoreDsl.scenario;
import static io.gatling.javaapi.http.HttpDsl.header;
import static io.gatling.javaapi.http.HttpDsl.http;
import static io.gatling.javaapi.http.HttpDsl.status;

import io.gatling.javaapi.core.FeederBuilder;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 상품 조회에 부하를 주어 <b>처리량·지연</b>과 <b>인스턴스별 분배</b>를 함께 본다.
 *
 * <p>Gatling 은 "어느 인스턴스가 응답했는지"를 알지 못한다. 그래서 product-service 가
 * 모든 응답에 실어 보내는 {@code X-Instance-Id} 헤더를 받아 직접 센다.
 *
 * <p>실행:
 * <pre>
 *   docker compose up -d --build --scale product-service=2
 *   ./gradlew :load-test:gatlingRun
 * </pre>
 *
 * <p>대상 주소는 {@code -DbaseUrl=...} 으로 바꿀 수 있다.
 */
public class ProductBrowseSimulation extends Simulation {

    /** 인스턴스별 처리 건수. 가상 사용자들이 동시에 더하므로 동시성 안전한 자료구조를 쓴다. */
    private static final Map<String, LongAdder> HITS = new ConcurrentHashMap<>();

    private static final String BASE_URL =
            System.getProperty("baseUrl", "http://localhost:8080");

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE_URL)
            .acceptHeader("application/json")
            .userAgentHeader("gatling-load-test");

    /** 같은 상품만 조회하면 캐시 효과가 섞이므로 세 상품을 돌아가며 조회한다. */
    private final FeederBuilder<Object> products = listFeeder(List.of(
            Map.of("productId", 1),
            Map.of("productId", 2),
            Map.of("productId", 3))).circular();

    private final ScenarioBuilder browse = scenario("상품 조회")
            .feed(products)
            .exec(http("GET /api/products/{id}")
                    .get("/api/products/#{productId}")
                    .check(status().is(200))
                    // 응답 헤더에서 처리 인스턴스를 꺼내 세션에 담는다.
                    .check(header("X-Instance-Id").saveAs("instance")))
            .exec(session -> {
                String instance = session.getString("instance");
                if (instance != null) {
                    HITS.computeIfAbsent(instance, key -> new LongAdder()).increment();
                }
                return session;
            });

    {
        setUp(browse.injectOpen(
                        // 10초에 걸쳐 50명까지 늘린 뒤(램프업), 20초 동안 초당 30건을 유지한다.
                        // 램프업을 두는 이유는 첫 요청에 몰리는 커넥션 수립과 JIT 예열이
                        // 지연 통계를 왜곡하기 때문이다.
                        rampUsers(50).during(Duration.ofSeconds(10)),
                        constantUsersPerSec(30).during(Duration.ofSeconds(20))))
                .protocols(httpProtocol)
                .assertions(
                        global().failedRequests().percent().lt(1.0),
                        details("GET /api/products/{id}").responseTime().percentile3().lt(1000));
    }

    @Override
    public void after() {
        long total = HITS.values().stream().mapToLong(LongAdder::sum).sum();
        System.out.println();
        System.out.println("=== 인스턴스별 처리 건수 (총 " + total + "건) ===");
        HITS.forEach((instance, count) -> System.out.printf("  %-30s %6d  (%.1f%%)%n",
                instance, count.sum(), count.sum() * 100.0 / total));
        System.out.println();
    }
}

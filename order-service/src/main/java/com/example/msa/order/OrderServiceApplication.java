package com.example.msa.order;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

// @EnableFeignClients: @FeignClient 인터페이스를 찾아 HTTP 호출 구현체를 만들어 준다.
// @EnableScheduling: SagaTimeoutSweeper 의 @Scheduled 를 실제로 돌린다. 이게 없으면
// 애너테이션만 붙어 있고 아무 일도 일어나지 않는다.
@EnableFeignClients
@EnableScheduling
@SpringBootApplication
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    /**
     * 토픽을 코드로 선언해 둔다. Kafka 는 없는 토픽에 메시지가 오면 자동 생성해 주지만,
     * 그 설정은 운영 환경에서 대개 꺼져 있어 의존하면 안 된다.
     *
     * <p>파티션이 1개이므로 product-service 를 여러 개 띄워도 명령을 실제로 소비하는
     * 인스턴스는 하나뿐이다. 같은 컨슈머 그룹 안에서 하나의 파티션은 한 인스턴스에만
     * 배정되기 때문이다. 소비까지 나누려면 파티션 수를 늘리고, 그때도 <b>같은 주문의
     * 명령은 같은 파티션에 가야</b> 하므로 메시지 키를 orderId 로 두는 것이 전제가 된다.
     */
    @Bean
    NewTopic stockCommandTopic() {
        return TopicBuilder.name(StockCommand.TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic paymentCommandTopic() {
        return TopicBuilder.name(PaymentCommand.TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic sagaReplyTopic() {
        return TopicBuilder.name(SagaReply.TOPIC).partitions(1).replicas(1).build();
    }
}

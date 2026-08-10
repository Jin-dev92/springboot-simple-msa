package com.example.msa.product;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

// @EnableScheduling: OutboxRelay 의 @Scheduled 를 실제로 돌린다.
@EnableScheduling
@SpringBootApplication
public class ProductServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProductServiceApplication.class, args);
    }

    @Bean
    NewTopic productChangedTopic() {
        return TopicBuilder.name(ProductChangedEvent.TOPIC).partitions(1).replicas(1).build();
    }

    /**
     * 기본 발행기. <b>직접 선언해야 하는 이유가 있다.</b>
     *
     * <p>스프링 부트는 {@code KafkaTemplate} 빈이 <b>하나도 없을 때만</b> 기본 것을
     * 만들어 준다({@code @ConditionalOnMissingBean}). 아래에서 릴레이용 템플릿을
     * 선언하는 순간 자동 설정이 물러나므로, 그동안 자동으로 받아 쓰던
     * {@code StockCommandListener} 가 주입 대상을 잃는다. 하나를 만들면 나머지도
     * 직접 만들어야 한다.
     *
     * <p>{@code spring.kafka.template.observation-enabled} 도 자동 설정용이므로
     * 여기서 직접 켠다. 이걸 빠뜨리면 Zipkin 에서 추적이 끊긴다.
     */
    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(KafkaProperties properties) {
        KafkaTemplate<String, Object> template = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(properties.buildProducerProperties(null)));
        template.setObservationEnabled(true);
        return template;
    }

    /**
     * {@link OutboxRelay} 전용 발행기. 값 직렬화기만 다르다.
     *
     * <p>outbox 에 담긴 것은 이미 JSON 문자열이다. 기본 발행기({@code JsonSerializer})로
     * 보내면 문자열을 한 번 더 감싸 나가고 받는 쪽이 역직렬화에 실패한다.
     *
     * <p>추적을 켜 두지만, <b>이 발행의 span 은 원래 요청이 아니라 릴레이에 붙습니다.</b>
     * outbox 를 거치는 순간 발행이 요청과 다른 스레드·다른 시점으로 떨어지기 때문이다.
     * 유실을 막는 대가로 추적의 연결이 한 번 끊기는 셈이다.
     */
    @Bean
    KafkaTemplate<String, String> outboxKafkaTemplate(KafkaProperties properties) {
        Map<String, Object> config = new HashMap<>(properties.buildProducerProperties(null));
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate<String, String> template =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
        template.setObservationEnabled(true);
        return template;
    }
}

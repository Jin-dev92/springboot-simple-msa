package com.example.msa.order;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

/**
 * 두 번째 메시지 타입을 구독하기 위한 설정.
 *
 * <p>이 서비스는 Phase 11 부터 두 종류를 받는다 — {@code saga-reply}(참여자 응답)와
 * {@code product-changed}(상품 변경 사실). 그런데 발신측이 타입 헤더를 붙이지 않는
 * 방침이라({@code spring.json.add.type.headers: false}) <b>수신측이 타입을 정해야</b>
 * 하고, 전역 설정 {@code spring.json.value.default.type} 은 값이 하나뿐이다.
 *
 * <p>그래서 새 리스너만 다른 역직렬화기를 쓰도록 컨테이너 팩토리를 따로 만든다.
 * 기존 {@code saga-reply} 리스너는 전역 설정을 그대로 쓴다.
 *
 * <p>타입 헤더를 켜면 이 설정이 필요 없어지지만, 그러면 수신측이 발신측과 같은
 * 패키지·클래스 이름을 갖도록 강요받는다. <b>공통 모듈을 두지 않기로 한 대가를
 * 여기서 한 번 더 지불하는 셈</b>이다.
 */
@Configuration
class KafkaConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, ProductChangedEvent>
            productChangedListenerContainerFactory(KafkaProperties properties) {

        JsonDeserializer<ProductChangedEvent> valueDeserializer =
                new JsonDeserializer<>(ProductChangedEvent.class);
        // 발신측이 타입 헤더를 붙이지 않으므로, 헤더가 있더라도 무시하고
        // 위에서 지정한 타입으로 읽는다.
        valueDeserializer.ignoreTypeHeaders();

        Map<String, Object> config = properties.buildConsumerProperties(null);
        // 역직렬화기를 코드로 만들어 넘기므로, 같은 것을 설정으로도 지시하면 안 된다.
        // 스프링 카프카는 둘을 함께 주면 기동 시점에 거부한다
        // ("must be configured with property setters, or via configuration properties; not both").
        // application.yml 의 spring.json.* 은 saga-reply 리스너용이므로 여기서만 걷어낸다.
        config.keySet().removeIf(k -> k.startsWith("spring.json"));
        config.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        config.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        // 복제본은 처음부터 다시 쌓아야 맞으므로, 이 그룹은 토픽의 맨 앞부터 읽는다.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        DefaultKafkaConsumerFactory<String, ProductChangedEvent> consumerFactory =
                new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                        valueDeserializer);

        ConcurrentKafkaListenerContainerFactory<String, ProductChangedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // 발신측이 실어 보낸 추적 정보를 이어받는다. 전역 리스너 설정과 맞춘다.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}

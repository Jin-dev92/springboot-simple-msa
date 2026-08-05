package com.example.msa.order;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

/**
 * product-service 를 실제로 띄우지 않고 Feign 클라이언트만 가짜로 바꿔치기한다.
 * 검증 대상은 "다른 서비스에서 받아온 가격으로 총액을 계산해 저장하는가" 이다.
 */
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.admin.auto-create=false"
})
@AutoConfigureMockMvc
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ProductClient productClient;

    // 브로커가 없으므로 발행도 가짜로 바꾼다. 대신 "무엇을 발행했는지"는 검증한다.
    @MockitoBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void 주문을_생성하면_상품_가격으로_총액을_계산한다() throws Exception {
        given(productClient.findById(1L)).willReturn(
                new ProductClient.ProductResponse(1L, "키보드", new BigDecimal("89000"), 30));

        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 3}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.productId").value(1))
                .andExpect(jsonPath("$.totalPrice").value(267000));

        org.mockito.Mockito.verify(kafkaTemplate).send(
                org.mockito.ArgumentMatchers.eq(OrderCreatedEvent.TOPIC),
                org.mockito.ArgumentMatchers.argThat(event ->
                        event instanceof OrderCreatedEvent e
                                && e.productId() == 1L && e.quantity() == 3));
    }

    @Test
    void 수량이_0이면_400을_반환하고_상품_조회조차_하지_않는다() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 0}
                                """))
                .andExpect(status().isBadRequest());

        org.mockito.Mockito.verify(productClient, org.mockito.Mockito.never()).findById(any());
    }
}

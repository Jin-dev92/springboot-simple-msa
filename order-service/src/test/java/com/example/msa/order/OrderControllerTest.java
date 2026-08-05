package com.example.msa.order;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
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
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/**
 * product-service 를 실제로 띄우지 않고 Feign 클라이언트만 가짜로 바꿔치기한다.
 * 검증 대상은 "다른 서비스에서 받아온 가격으로 총액을 계산해 저장하는가"와
 * "남의 주문이 보이지 않는가" 두 가지다.
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

    /** uid 클레임을 담은 토큰으로 인증된 요청을 만든다. 실제 서명은 필요 없다. */
    private static <T extends MockHttpServletRequestBuilder> T asUser(T request, long userId) {
        request.with(jwt().jwt(j -> j.claim("uid", userId).subject("user-" + userId)));
        return request;
    }

    @Test
    void 주문을_생성하면_상품_가격으로_총액을_계산한다() throws Exception {
        given(productClient.findById(1L)).willReturn(
                new ProductClient.ProductResponse(1L, "키보드", new BigDecimal("89000"), 30));

        mockMvc.perform(asUser(post("/orders"), 7L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 3}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.productId").value(1))
                .andExpect(jsonPath("$.userId").value(7))
                .andExpect(jsonPath("$.totalPrice").value(267000));

        org.mockito.Mockito.verify(kafkaTemplate).send(
                org.mockito.ArgumentMatchers.eq(OrderCreatedEvent.TOPIC),
                org.mockito.ArgumentMatchers.argThat(event ->
                        event instanceof OrderCreatedEvent e
                                && e.productId() == 1L && e.quantity() == 3));
    }

    @Test
    void 수량이_0이면_400을_반환하고_상품_조회조차_하지_않는다() throws Exception {
        mockMvc.perform(asUser(post("/orders"), 7L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 0}
                                """))
                .andExpect(status().isBadRequest());

        org.mockito.Mockito.verify(productClient, org.mockito.Mockito.never()).findById(any());
    }

    @Test
    void 토큰이_없으면_401을_반환한다() throws Exception {
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 1}
                                """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void 남의_주문은_목록에_보이지_않는다() throws Exception {
        given(productClient.findById(1L)).willReturn(
                new ProductClient.ProductResponse(1L, "키보드", new BigDecimal("89000"), 30));

        // 사용자 100 이 주문을 만든다
        mockMvc.perform(asUser(post("/orders"), 100L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 1}
                                """))
                .andExpect(status().isCreated());

        // 사용자 200 이 목록을 조회하면 비어 있어야 한다
        mockMvc.perform(asUser(get("/orders"), 200L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(0));

        // 본인은 자기 주문을 볼 수 있다
        mockMvc.perform(asUser(get("/orders"), 100L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }
}

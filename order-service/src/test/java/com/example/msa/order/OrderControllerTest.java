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
        "spring.kafka.admin.auto-create=false",
        "saga.timeout.check-interval=1h"
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
                .andExpect(jsonPath("$.productName").value("키보드"))
                .andExpect(jsonPath("$.userId").value(7))
                .andExpect(jsonPath("$.totalPrice").value(267000));

        org.mockito.Mockito.verify(kafkaTemplate).send(
                org.mockito.ArgumentMatchers.eq(StockCommand.TOPIC),
                org.mockito.ArgumentMatchers.any(String.class),
                org.mockito.ArgumentMatchers.argThat(command ->
                        command instanceof StockCommand c
                                && c.productId() == 1L && c.quantity() == 3
                                && c.action() == StockCommand.Action.RESERVE));
    }

    /**
     * Phase 9 의 검증 기준. 이 테스트가 이 단계의 존재 이유다.
     *
     * <p>상품명을 조인해 왔다면 과거 주문이 현재 상품을 따라 움직인다. 주문 시점에
     * 복사해 두었으므로 상품이 이름을 바꿔도 영수증은 그대로여야 한다.
     */
    @Test
    void 상품명이_바뀌어도_과거_주문의_상품명은_그대로다() throws Exception {
        given(productClient.findById(1L)).willReturn(
                new ProductClient.ProductResponse(1L, "키보드", new BigDecimal("89000"), 30));

        mockMvc.perform(asUser(post("/orders"), 42L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"productId": 1, "quantity": 1}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.productName").value("키보드"));

        // product-service 에서 상품 이름이 바뀌었다.
        given(productClient.findById(1L)).willReturn(
                new ProductClient.ProductResponse(1L, "무선 키보드", new BigDecimal("89000"), 30));

        mockMvc.perform(asUser(get("/orders"), 42L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].productName").value("키보드"));
    }

    /**
     * 조회 경로가 product-service 에 의존하지 않는다는 것을 확인한다.
     *
     * <p>Feign 클라이언트를 아예 부르지 않으므로, 주문이 N 건이든 호출은 0 회다.
     * 조인을 없앤 것이 아니라 <b>조인할 필요 자체를 없앤 것</b>이 요점이다.
     */
    @Test
    void 주문_목록_조회는_product_service_를_부르지_않는다() throws Exception {
        given(productClient.findById(1L)).willReturn(
                new ProductClient.ProductResponse(1L, "키보드", new BigDecimal("89000"), 30));

        for (int i = 0; i < 3; i++) {
            mockMvc.perform(asUser(post("/orders"), 43L)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {"productId": 1, "quantity": 1}
                                    """))
                    .andExpect(status().isCreated());
        }
        org.mockito.Mockito.clearInvocations(productClient);

        mockMvc.perform(asUser(get("/orders"), 43L))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3))
                .andExpect(jsonPath("$[0].productName").value("키보드"));

        org.mockito.Mockito.verify(productClient, org.mockito.Mockito.never()).findById(any());
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

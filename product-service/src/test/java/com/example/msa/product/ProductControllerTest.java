package com.example.msa.product;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;

// 테스트는 레지스트리도 브로커도 없이 이 서비스만 단독으로 검증한다.
@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
@AutoConfigureMockMvc
class ProductControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void 상품을_id로_조회한다() throws Exception {
        mockMvc.perform(get("/products/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("키보드"))
                .andExpect(jsonPath("$.price").value(89000));
    }

    @Test
    void 없는_상품은_404를_반환한다() throws Exception {
        mockMvc.perform(get("/products/9999"))
                .andExpect(status().isNotFound());
    }

    @Test
    void 모든_응답에_인스턴스_식별자가_붙는다(@Autowired InstanceId instanceId) throws Exception {
        // 성공 응답과 에러 응답 모두에 붙어야 분포 집계가 어긋나지 않는다.
        mockMvc.perform(get("/products/1"))
                .andExpect(header().string("X-Instance-Id", instanceId.value()));

        mockMvc.perform(get("/products/9999"))
                .andExpect(header().string("X-Instance-Id", instanceId.value()));
    }

    @Test
    void 상품_조회는_토큰_없이도_가능하다() throws Exception {
        // 카탈로그는 공개 정보다. 로그인을 요구하면 오히려 서비스가 불편해진다.
        mockMvc.perform(get("/products"))
                .andExpect(status().isOk());
    }

    @Test
    void 상품_등록은_토큰이_없으면_401() throws Exception {
        mockMvc.perform(post("/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(NEW_PRODUCT))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void 상품_등록은_USER_권한이면_403() throws Exception {
        // 인증은 됐지만 권한이 부족한 경우. 401(누구인지 모름)과 403(알지만 안 됨)은 다르다.
        mockMvc.perform(post("/products")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_USER")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(NEW_PRODUCT))
                .andExpect(status().isForbidden());
    }

    @Test
    void 상품_등록은_ADMIN_권한이면_성공한다() throws Exception {
        mockMvc.perform(post("/products")
                        .with(jwt().authorities(new SimpleGrantedAuthority("ROLE_ADMIN")))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(NEW_PRODUCT))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("스피커"));
    }

    private static final String NEW_PRODUCT = """
            {"name": "스피커", "price": 120000, "stock": 5}
            """;
}

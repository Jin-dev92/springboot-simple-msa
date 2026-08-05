package com.example.msa.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import javax.crypto.SecretKey;

@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false"
})
@AutoConfigureMockMvc
class AuthControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SecretKey jwtSecretKey;

    @Test
    void 올바른_비밀번호로_로그인하면_토큰을_받는다() throws Exception {
        MvcResult result = mockMvc.perform(post("/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"username": "admin", "password": "admin123"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.role").value("ROLE_ADMIN"))
                .andReturn();

        // 발급된 토큰이 실제로 검증 가능한지, 클레임이 제대로 들어갔는지 확인한다.
        String token = com.jayway.jsonpath.JsonPath.read(
                result.getResponse().getContentAsString(), "$.accessToken");

        JwtDecoder decoder = NimbusJwtDecoder.withSecretKey(jwtSecretKey).build();
        Jwt jwt = decoder.decode(token);

        assertThat(jwt.getSubject()).isEqualTo("admin");
        assertThat(jwt.getClaimAsStringList("roles")).containsExactly("ROLE_ADMIN");
        // getClaim 은 제네릭 반환이라 캐스팅해야 assertThat 오버로드가 정해진다.
        assertThat((Object) jwt.getClaim("uid")).isNotNull();
    }

    @Test
    void 비밀번호가_틀리면_401() throws Exception {
        mockMvc.perform(post("/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"username": "admin", "password": "wrong"}
                                """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void 없는_사용자도_같은_401을_받는다() throws Exception {
        // 존재하지 않는 계정과 비밀번호 오류를 구분해 알려주면
        // "이 아이디는 존재한다"는 정보가 새어 나간다(사용자 열거 공격).
        mockMvc.perform(post("/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"username": "nobody", "password": "whatever"}
                                """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void 비밀번호는_해시로_저장된다(@Autowired AppUserRepository repository) {
        String stored = repository.findByUsername("user").orElseThrow().getPassword();

        assertThat(stored).isNotEqualTo("user123");
        assertThat(stored).startsWith("$2a$");   // BCrypt 해시 형식
    }
}

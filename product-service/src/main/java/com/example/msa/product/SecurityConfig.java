package com.example.msa.product;

import java.nio.charset.StandardCharsets;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * 이 서비스는 게이트웨이 뒤에 있고 호스트 포트도 열려 있지 않다. 그런데도 토큰을 다시
 * 검증한다. 게이트웨이를 통과했다는 사실 자체를 믿지 않는 것이다(zero trust).
 *
 * <p>게이트웨이만 믿는 구조에서는, 내부 네트워크에 접근할 수 있게 된 공격자나 잘못
 * 열린 경로 하나가 곧바로 전체 데이터 접근으로 이어진다. 검증은 데이터를 실제로
 * 가지고 있는 쪽에서 해야 한다.
 */
@Configuration
class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // 상품 조회는 공개. 로그인하지 않아도 카탈로그는 볼 수 있어야 한다.
                        .requestMatchers(HttpMethod.GET, "/products", "/products/**").permitAll()
                        // 상품 등록은 관리자만.
                        .requestMatchers(HttpMethod.POST, "/products").hasRole("ADMIN")
                        .anyRequest().authenticated())
                .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt
                        .jwtAuthenticationConverter(jwtAuthenticationConverter())))
                .build();
    }

    @Bean
    JwtDecoder jwtDecoder(@Value("${jwt.secret}") String secret) {
        return NimbusJwtDecoder
                .withSecretKey(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
                .build();
    }

    /**
     * 토큰의 {@code roles} 클레임을 스프링 시큐리티의 권한으로 옮긴다.
     *
     * <p>기본 동작은 {@code scope} 클레임을 읽고 {@code SCOPE_} 접두사를 붙이는 것이라
     * 이 프로젝트의 토큰 형태와 맞지 않는다. 클레임 이름을 roles 로 바꾸고, 값에 이미
     * ROLE_ 이 붙어 있으므로 접두사는 빈 문자열로 둔다.
     */
    private static JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtGrantedAuthoritiesConverter authorities = new JwtGrantedAuthoritiesConverter();
        authorities.setAuthoritiesClaimName("roles");
        authorities.setAuthorityPrefix("");

        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(authorities);
        return converter;
    }
}

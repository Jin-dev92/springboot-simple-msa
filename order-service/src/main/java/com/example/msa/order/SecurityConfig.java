package com.example.msa.order;

import java.nio.charset.StandardCharsets;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * product-service 와 마찬가지로 게이트웨이를 통과했다는 사실을 신뢰하지 않고 다시 검증한다.
 *
 * <p>주문은 공개 데이터가 아니므로 모든 경로가 인증을 요구한다. 다만 <b>인증만으로는
 * 부족하다</b>. 로그인한 사용자 누구나 남의 주문을 볼 수 있으면 안 되기 때문이다.
 * 그 판단은 여기서 할 수 없고 {@link OrderController} 에서 사용자 id 로 걸러 낸다.
 */
@Configuration
class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        return http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // 주문은 전부 인증 필요. 역할 구분은 두지 않는다.
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

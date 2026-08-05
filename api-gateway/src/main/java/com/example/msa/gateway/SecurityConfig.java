package com.example.msa.gateway;

import java.nio.charset.StandardCharsets;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.oauth2.jwt.NimbusReactiveJwtDecoder;
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.oauth2.server.resource.authentication.ReactiveJwtAuthenticationConverterAdapter;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * 첫 번째 검문소.
 *
 * <p>게이트웨이는 WebFlux(리액티브) 기반이라 설정 타입이 다른 두 서비스와 다르다.
 * {@code HttpSecurity} 대신 {@code ServerHttpSecurity}, {@code JwtDecoder} 대신
 * {@code ReactiveJwtDecoder} 를 쓴다. 판단 기준 자체는 동일하다.
 *
 * <p>여기서 막으면 잘못된 요청이 내부 네트워크에 아예 들어오지 못한다. 그래도
 * 내부 서비스들이 각자 다시 검증하는 이유는, 이 검문소를 우회하는 경로가 언젠가
 * 생길 수 있다고 전제하기 때문이다.
 */
@Configuration
@EnableWebFluxSecurity
class SecurityConfig {

    @Bean
    SecurityWebFilterChain securityWebFilterChain(ServerHttpSecurity http) {
        return http
                .csrf(ServerHttpSecurity.CsrfSpec::disable)
                .httpBasic(ServerHttpSecurity.HttpBasicSpec::disable)
                .formLogin(ServerHttpSecurity.FormLoginSpec::disable)
                .authorizeExchange(exchange -> exchange
                        // 로그인은 토큰을 받기 위한 통로이므로 당연히 공개여야 한다.
                        .pathMatchers("/api/auth/**").permitAll()
                        // 상품 카탈로그 조회는 공개.
                        .pathMatchers(HttpMethod.GET, "/api/products/**").permitAll()
                        // 상품 등록은 관리자만.
                        .pathMatchers(HttpMethod.POST, "/api/products").hasRole("ADMIN")
                        // 나머지(주문 등)는 전부 인증 필요.
                        .anyExchange().authenticated())
                .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt
                        .jwtAuthenticationConverter(
                                new ReactiveJwtAuthenticationConverterAdapter(rolesConverter()))))
                .build();
    }

    @Bean
    ReactiveJwtDecoder jwtDecoder(@Value("${jwt.secret}") String secret) {
        return NimbusReactiveJwtDecoder
                .withSecretKey(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
                .build();
    }

    /**
     * 토큰의 {@code roles} 클레임을 스프링 시큐리티의 권한으로 옮긴다.
     * 기본 동작은 {@code scope} 클레임에 {@code SCOPE_} 접두사를 붙이는 것이라 맞지 않는다.
     */
    private static JwtAuthenticationConverter rolesConverter() {
        JwtGrantedAuthoritiesConverter authorities = new JwtGrantedAuthoritiesConverter();
        authorities.setAuthoritiesClaimName("roles");
        authorities.setAuthorityPrefix("");

        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(authorities);
        return converter;
    }
}

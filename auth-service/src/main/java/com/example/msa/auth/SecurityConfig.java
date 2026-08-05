package com.example.msa.auth;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        return http
                // CSRF 방어는 브라우저가 쿠키를 자동으로 실어 보내기 때문에 필요한 것이다.
                // 토큰을 헤더에 직접 담는 방식에는 그 문제가 없으므로 끈다.
                .csrf(csrf -> csrf.disable())
                // 서버가 세션을 만들지 않는다. 토큰 하나로 매 요청이 독립적으로 검증되므로,
                // 서비스 인스턴스를 늘려도 세션을 공유할 필요가 없다.
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/auth/login").permitAll()
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults())
                .build();
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * 토큰에 서명할 때 쓰는 열쇠.
     *
     * <p>HS256 은 <b>대칭키</b> 방식이라 서명과 검증에 같은 열쇠를 쓴다. 즉 검증만 하면 되는
     * product-service 나 order-service 도 <b>토큰을 발급할 수 있는 열쇠</b>를 갖게 된다.
     * 서비스 하나가 뚫리면 공격자가 임의의 ADMIN 토큰을 만들어 낼 수 있다는 뜻이다.
     *
     * <p>실무에서는 비대칭키(RS256)를 써서 auth-service 만 개인키로 서명하고, 나머지는
     * 공개키로 검증만 하게 만든다. 이 프로젝트는 열쇠 배포 과정을 단순화하기 위해
     * 대칭키를 택했고, 그 한계를 여기에 남겨 둔다.
     */
    @Bean
    JwtEncoder jwtEncoder(SecretKey jwtSecretKey) {
        return new NimbusJwtEncoder(new ImmutableSecret<>(jwtSecretKey));
    }

    @Bean
    SecretKey jwtSecretKey(@Value("${jwt.secret}") String secret) {
        // HS256 은 256비트(32바이트) 이상의 열쇠를 요구한다. 짧으면 기동 시 예외가 난다.
        return new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }
}

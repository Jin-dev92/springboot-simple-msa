package com.example.msa.auth;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Component;

/**
 * 로그인에 성공한 사용자에게 발급할 JWT 를 만든다.
 *
 * <p>JWT(JSON Web Token)는 사용자 정보를 담고 서명이 붙은 문자열이다. 서버가 세션을
 * 들고 있지 않아도, 토큰의 서명만 검증하면 내용을 믿을 수 있다. 서비스가 여러 개인
 * 환경에서 각 서비스가 세션 저장소를 공유하지 않아도 되는 것이 이 방식의 이점이다.
 *
 * <p>담기는 내용은 누구나 열어볼 수 있다는 점에 주의해야 한다. JWT 는 <b>서명</b>되어
 * 있을 뿐 <b>암호화</b>되어 있지 않다. 변조는 막지만 열람은 막지 못하므로,
 * 비밀번호나 개인정보를 클레임에 담아서는 안 된다.
 */
@Component
class JwtIssuer {

    private static final Duration TOKEN_LIFETIME = Duration.ofHours(1);

    private final JwtEncoder jwtEncoder;

    JwtIssuer(JwtEncoder jwtEncoder) {
        this.jwtEncoder = jwtEncoder;
    }

    String issue(AppUser user) {
        Instant now = Instant.now();

        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("auth-service")
                .issuedAt(now)
                .expiresAt(now.plus(TOKEN_LIFETIME))
                .subject(user.getUsername())
                // uid: 다른 서비스가 "누구의 데이터인가"를 판단할 때 쓴다.
                // 예를 들어 order-service 는 이 값으로 본인 주문만 골라낸다.
                .claim("uid", user.getId())
                .claim("roles", List.of(user.getRole()))
                .build();

        JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
        return jwtEncoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }

    long lifetimeSeconds() {
        return TOKEN_LIFETIME.toSeconds();
    }
}

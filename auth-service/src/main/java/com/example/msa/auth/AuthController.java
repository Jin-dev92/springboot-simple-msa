package com.example.msa.auth;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/auth")
class AuthController {

    private static final Logger log = LoggerFactory.getLogger(AuthController.class);

    private final AppUserRepository repository;
    private final PasswordEncoder passwordEncoder;
    private final JwtIssuer jwtIssuer;

    AuthController(AppUserRepository repository, PasswordEncoder passwordEncoder, JwtIssuer jwtIssuer) {
        this.repository = repository;
        this.passwordEncoder = passwordEncoder;
        this.jwtIssuer = jwtIssuer;
    }

    @PostMapping("/login")
    LoginResponse login(@Valid @RequestBody LoginRequest request) {
        AppUser user = repository.findByUsername(request.username())
                .filter(u -> passwordEncoder.matches(request.password(), u.getPassword()))
                .orElseThrow(() -> {
                    // 실패 사유를 응답에 나누어 알려주면 공격자에게 "이 아이디는 존재한다"는
                    // 정보를 주게 된다(사용자 열거 공격). 두 경우 모두 같은 메시지를 쓴다.
                    log.warn("로그인 실패: username={}", request.username());
                    return new ResponseStatusException(HttpStatus.UNAUTHORIZED,
                            "아이디 또는 비밀번호가 올바르지 않습니다");
                });

        log.info("로그인 성공: username={}, role={}", user.getUsername(), user.getRole());
        return new LoginResponse(jwtIssuer.issue(user), jwtIssuer.lifetimeSeconds(), user.getRole());
    }

    record LoginRequest(@NotBlank String username, @NotBlank String password) {
    }

    record LoginResponse(String accessToken, long expiresIn, String role) {
    }
}

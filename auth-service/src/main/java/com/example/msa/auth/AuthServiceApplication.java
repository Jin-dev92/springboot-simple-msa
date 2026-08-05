package com.example.msa.auth;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.security.crypto.password.PasswordEncoder;

@SpringBootApplication
public class AuthServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuthServiceApplication.class, args);
    }

    /**
     * 학습용 초기 계정을 심는다.
     *
     * <p>다른 서비스처럼 data.sql 로 넣지 않는 이유는, 비밀번호를 저장하려면 먼저
     * 해시로 바꿔야 하는데 그 일은 애플리케이션 코드만 할 수 있기 때문이다.
     * SQL 에 해시값을 직접 박아 넣으면 그 값이 어디서 온 것인지 알 수 없게 된다.
     */
    @Bean
    CommandLineRunner seedUsers(AppUserRepository repository, PasswordEncoder encoder) {
        return args -> {
            repository.save(new AppUser("user", encoder.encode("user123"), "ROLE_USER"));
            repository.save(new AppUser("admin", encoder.encode("admin123"), "ROLE_ADMIN"));
        };
    }
}

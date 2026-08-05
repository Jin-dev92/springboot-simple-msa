package com.example.msa.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 외부로 열리는 유일한 진입점.
 *
 * <p>클라이언트는 내부 서비스가 몇 개인지, 어느 포트에 떠 있는지 알 필요가 없다.
 * 라우팅 규칙은 코드가 아니라 application.yml 에 선언되어 있다.
 */
@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}

package com.example.msa.discovery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * 서비스 레지스트리(전화번호부) 역할을 하는 Eureka 서버.
 *
 * <p>다른 서비스들은 기동할 때 자신의 이름과 실제 주소를 이곳에 등록하고,
 * 주기적으로 heartbeat 를 보내 살아 있음을 알린다. 호출하는 쪽은 상대의 IP 대신
 * 서비스 이름만 알면 되고, 실제 주소는 이 레지스트리에서 조회한다.
 */
@EnableEurekaServer
@SpringBootApplication
public class DiscoveryServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DiscoveryServerApplication.class, args);
    }
}

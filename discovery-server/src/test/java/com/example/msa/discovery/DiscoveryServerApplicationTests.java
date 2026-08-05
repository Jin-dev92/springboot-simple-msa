package com.example.msa.discovery;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "server.port=0")
class DiscoveryServerApplicationTests {

    @Test
    void 컨텍스트가_기동된다() {
        // Eureka 서버 설정이 깨지면 컨텍스트 로딩 단계에서 실패한다.
    }
}

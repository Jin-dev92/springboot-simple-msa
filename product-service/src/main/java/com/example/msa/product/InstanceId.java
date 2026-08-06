package com.example.msa.product;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.springframework.stereotype.Component;

/**
 * 이 프로세스를 가리키는 식별자. 기동 시 한 번 정해지고 이후 바뀌지 않는다.
 *
 * <p>인스턴스를 여러 개 띄웠을 때 <b>어느 쪽이 응답했는지</b>를 밖에서 알 수 있게 하려고
 * 둔다. 이것이 없으면 부하를 준 뒤 분포를 확인할 방법이 컨테이너 로그를 문자열로
 * 긁는 것뿐인데, 그 방식은 로그 형식과 셸 환경에 의존해 쉽게 깨진다.
 *
 * <p>호스트명은 도커에서 컨테이너 ID 가 되므로 {@code docker ps} 결과와 바로 대조된다.
 * 다만 한 머신에서 여러 인스턴스를 띄우면 호스트명이 같으므로, 짧은 임의값을 덧붙여
 * 프로세스마다 반드시 달라지게 한다.
 */
@Component
public class InstanceId {

    private final String value;

    InstanceId() {
        this.value = hostname() + "-" + UUID.randomUUID().toString().substring(0, 6);
    }

    public String value() {
        return value;
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown-host";
        }
    }
}

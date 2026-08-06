package com.example.msa.product;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * 모든 응답에 어느 인스턴스가 처리했는지를 헤더로 붙인다.
 *
 * <p>컨트롤러마다 넣지 않고 필터에 둔 이유는, 에러 응답을 포함해 <b>빠짐없이</b>
 * 붙어야 하기 때문이다. 분포를 셀 때 일부 응답에만 헤더가 있으면 집계가 어긋난다.
 *
 * <p>체인을 타기 전에 헤더를 먼저 세팅한다. 응답이 커밋된 뒤에는 헤더를 더 이상
 * 추가할 수 없다.
 */
@Component
class InstanceIdHeaderFilter extends OncePerRequestFilter {

    static final String HEADER = "X-Instance-Id";

    private final InstanceId instanceId;

    InstanceIdHeaderFilter(InstanceId instanceId) {
        this.instanceId = instanceId;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
            FilterChain chain) throws ServletException, IOException {
        response.setHeader(HEADER, instanceId.value());
        chain.doFilter(request, response);
    }
}

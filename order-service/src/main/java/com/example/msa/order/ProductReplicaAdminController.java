package com.example.msa.order;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 복제본 재구축을 손으로 돌리기 위한 운영용 경로.
 *
 * <p>기동 시 자동 재구축({@link ProductReplicaStartupRebuild})은 "비어 있을 때"만
 * 동작한다. 그런데 <b>비어 있지 않지만 틀린</b> 경우가 있다 — 이벤트를 놓쳤거나,
 * 리스너 버그로 잘못 반영했거나, 복제본 스키마를 바꿨을 때다. 복제본은 자기 교정이
 * 되지 않으므로(14절) 그때 되돌릴 수단이 반드시 있어야 한다.
 *
 * <p>접근 제어는 여기가 아니라 {@link SecurityConfig} 에 선언되어 있다(ADMIN).
 */
@RestController
@RequestMapping("/orders/admin")
class ProductReplicaAdminController {

    private final ProductReplicaRebuilder rebuilder;

    ProductReplicaAdminController(ProductReplicaRebuilder rebuilder) {
        this.rebuilder = rebuilder;
    }

    @PostMapping("/product-replica/rebuild")
    RebuildResult rebuild() {
        return new RebuildResult(rebuilder.rebuild());
    }

    record RebuildResult(int restored) {
    }
}

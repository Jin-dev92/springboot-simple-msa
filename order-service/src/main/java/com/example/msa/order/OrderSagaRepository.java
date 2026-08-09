package com.example.msa.order;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

interface OrderSagaRepository extends JpaRepository<OrderSaga, Long> {

    /** 지정한 단계에 머물러 있으면서 마지막 갱신이 기준 시각보다 오래된 Saga. 타임아웃 대상이다. */
    List<OrderSaga> findByStepInAndUpdatedAtBefore(Collection<SagaStep> steps, Instant deadline);
}

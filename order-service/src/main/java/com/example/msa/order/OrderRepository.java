package com.example.msa.order;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

    /**
     * 본인 주문만 조회한다.
     *
     * <p>{@code findAll()} 을 쓰고 나중에 자바 코드에서 거르는 방식은 위험하다.
     * 거르는 코드를 빠뜨리면 그대로 전체 노출이 된다. 조회 단계에서부터 사용자를
     * 조건에 넣어, 실수로 남의 데이터를 가져올 여지 자체를 없앤다.
     */
    List<Order> findByUserId(Long userId);

    Optional<Order> findByIdAndUserId(Long id, Long userId);
}

package com.example.msa.order;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

interface OutboxRepository extends JpaRepository<OutboxMessage, Long> {

    /**
     * 아직 발행되지 않은 메시지를 <b>삽입 순서대로</b> 가져온다.
     *
     * <p>순서가 요건이다. 같은 주문의 RESERVE 와 RELEASE 가 뒤바뀌면 재고가 어긋난다.
     *
     * <p>한 번에 100건으로 끊는 이유는 밀린 메시지가 많을 때 한 주기가 끝없이 길어지지
     * 않게 하기 위해서다. 남은 것은 다음 주기가 이어 간다.
     */
    List<OutboxMessage> findTop100ByPublishedAtIsNullOrderByIdAsc();
}

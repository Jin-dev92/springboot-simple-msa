package com.example.msa.product;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * 발행 대기 중인 메시지. Transactional Outbox 의 저장소다.
 *
 * <p>이 테이블이 존재하는 이유는 하나다. <b>DB 커밋과 Kafka 발행은 원자적으로 묶이지
 * 않는다.</b> 둘은 다른 시스템이고 두 시스템에 걸친 트랜잭션은 없다. 그래서 어느
 * 순서로 하든 사이에 죽으면 어긋난다.
 *
 * <pre>
 *   발행 먼저 → 커밋 실패:  명령은 나갔는데 상태는 없다  (되돌릴 수 없다)
 *   커밋 먼저 → 발행 실패:  상태는 있는데 명령이 없다    (되돌릴 수 있다)
 * </pre>
 *
 * <p>발행을 커밋 뒤로 옮기는 것은 <b>덜 나쁜 쪽</b>을 택하는 것일 뿐이다. 그래도
 * 유실은 남는다. Outbox 는 그 남은 유실을 없앤다. 메시지를 브로커가 아니라
 * <b>같은 DB 의 테이블에</b> 쓰면, 상태 변경과 메시지가 하나의 트랜잭션에 들어간다.
 * 둘 다 커밋되거나 둘 다 롤백된다.
 *
 * <p>대신 발행이 늦어진다. 릴레이가 이 테이블을 읽어 실제로 내보내기 전까지는
 * 메시지가 나가지 않는다. <b>유실 가능성을 지연과 중복 가능성으로 바꾼 것</b>이며,
 * 중복은 참여자 쪽 멱등 기록이 이미 흡수한다.
 */
@Entity
@Table(name = "outbox")
public class OutboxMessage {

    /**
     * 자동 증가 id 를 그대로 발행 순서로 쓴다.
     *
     * <p>같은 상품의 변경이 순서대로 나가야 하므로 릴레이는 이 id 오름차순으로 읽어
     * 보낸다. 별도 순번 컬럼을 두지 않는 이유는 삽입 순서가 곧 발생 순서이기 때문이다.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;

    /** Kafka 메시지 키. 같은 상품의 변경을 같은 파티션에 넣어 순서를 지킨다. */
    private String messageKey;

    /**
     * 직렬화된 메시지 본문.
     *
     * <p>자바 객체가 아니라 JSON 문자열로 저장한다. 타입 정보를 남기지 않으므로
     * 나중에 record 이름이 바뀌어도 이미 쌓인 메시지를 읽을 수 있고, 서비스 간
     * 공통 모듈을 두지 않는 방침과도 어긋나지 않는다.
     */
    @Lob
    private String payload;

    private Instant createdAt;

    /** 발행된 시각. {@code null} 이면 아직 나가지 않은 것이다. */
    private Instant publishedAt;

    protected OutboxMessage() {
        // JPA 기본 생성자
    }

    OutboxMessage(String topic, String messageKey, String payload) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    void markPublished() {
        this.publishedAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}

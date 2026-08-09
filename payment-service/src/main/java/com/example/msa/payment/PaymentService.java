package com.example.msa.payment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 결제 판단. Saga 에서 이 서비스가 맡은 로컬 트랜잭션이다.
 *
 * <p>리스너와 별도의 빈으로 둔 데는 이유가 있다. {@code @Transactional} 은 스프링이
 * 만든 프록시를 거쳐야 동작하는데, <b>같은 클래스 안에서 자기 메서드를 호출하면
 * 프록시를 거치지 않아 트랜잭션이 걸리지 않는다</b>(self-invocation). 조용히 실패하기
 * 때문에 알아채기 어렵다. 트랜잭션 경계를 다른 빈으로 옮기면 이 문제가 사라진다.
 */
@Service
class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final AccountRepository accounts;
    private final ProcessedCommandRepository processed;

    PaymentService(AccountRepository accounts, ProcessedCommandRepository processed) {
        this.accounts = accounts;
        this.processed = processed;
    }

    /**
     * 잔액 차감과 처리 기록을 <b>하나의 트랜잭션</b>으로 묶는다.
     *
     * <p>따로 커밋하면 "잔액은 줄었는데 기록은 없는" 상태가 생기고, 그러면 같은
     * 명령이 다시 왔을 때 또 줄어든다. 둘이 같은 DB 안에 있으므로 묶을 수 있다.
     */
    // public 인 이유: @Transactional 이 non-public 메서드에도 걸리는지는 스프링 버전과
    // 프록시 방식에 따라 달라진다. 조용히 무시되면 알아채기 어려우므로 확실한 쪽을 택했다.
    // 클래스 자체가 package-private 이라 실제 노출 범위는 그대로다.
    @Transactional
    public SagaReply handle(PaymentCommand command) {
        // 이미 처리한 명령이면 잔액을 다시 건드리지 않고 그때의 결론을 그대로 돌려준다.
        // 그냥 건너뛰고 아무것도 돌려주지 않으면, 첫 처리에서 응답 발행이 실패했을 경우
        // Saga 가 영원히 대기 상태로 남는다.
        var seen = processed.findById(ProcessedCommand.idOf(command.orderId(), command.action()));
        if (seen.isPresent()) {
            ProcessedCommand record = seen.get();
            log.info("이미 처리한 명령. 잔액은 건드리지 않고 결과만 재발행: orderId={}, action={}, 최초처리={}",
                    command.orderId(), command.action(), record.getProcessedAt());
            return new SagaReply(command.orderId(), command.action().name(),
                    record.isSuccess(), record.getReason());
        }

        SagaReply reply = accounts.findById(command.userId())
                .map(account -> charge(account, command))
                .orElseGet(() -> {
                    log.warn("존재하지 않는 계좌에 대한 결제 명령: userId={}", command.userId());
                    return SagaReply.fail(command.orderId(), command.action(), "계좌가 없습니다");
                });

        processed.save(new ProcessedCommand(command.orderId(), command.action(),
                reply.success(), reply.reason()));
        return reply;
    }

    private SagaReply charge(Account account, PaymentCommand command) {
        if (!account.withdraw(command.amount())) {
            log.warn("잔액 부족: userId={}, 청구액={}, 잔액={} (orderId={})",
                    command.userId(), command.amount(), account.getBalance(), command.orderId());
            return SagaReply.fail(command.orderId(), command.action(),
                    "잔액 부족 (청구 %s, 잔액 %s)".formatted(command.amount(), account.getBalance()));
        }

        accounts.save(account);
        log.info("결제 완료: userId={}, 청구액={}, 남은잔액={} (orderId={})",
                command.userId(), command.amount(), account.getBalance(), command.orderId());
        return SagaReply.ok(command.orderId(), command.action());
    }
}

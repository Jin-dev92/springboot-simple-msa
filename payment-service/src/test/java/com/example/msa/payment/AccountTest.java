package com.example.msa.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "eureka.client.enabled=false",
        "management.tracing.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false"
})
class AccountTest {

    @Autowired
    private AccountRepository repository;

    @Test
    void 초기_계좌는_백만원을_가진다() {
        assertThat(repository.findById(1L).orElseThrow().getBalance())
                .isEqualByComparingTo("1000000");
        assertThat(repository.findById(2L).orElseThrow().getBalance())
                .isEqualByComparingTo("1000000");
    }

    @Test
    void 잔액을_넘는_출금은_거절하고_잔액을_건드리지_않는다() {
        Account account = new Account(99L, new BigDecimal("1000"));

        assertThat(account.withdraw(new BigDecimal("1001"))).isFalse();
        assertThat(account.getBalance()).isEqualByComparingTo("1000");
    }

    @Test
    void 잔액과_같은_금액은_출금할_수_있다() {
        Account account = new Account(99L, new BigDecimal("1000"));

        assertThat(account.withdraw(new BigDecimal("1000"))).isTrue();
        assertThat(account.getBalance()).isEqualByComparingTo("0");
    }

    @Test
    void 입금은_보상에_쓰이므로_잔액을_되돌린다() {
        Account account = new Account(99L, new BigDecimal("1000"));
        account.withdraw(new BigDecimal("400"));

        account.deposit(new BigDecimal("400"));

        assertThat(account.getBalance()).isEqualByComparingTo("1000");
    }
}

package com.example.msa.auth;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 사용자 계정.
 *
 * <p>클래스 이름을 {@code User} 로 짓지 않은 이유는 두 가지다. USER 는 SQL 예약어이고,
 * 스프링 시큐리티에도 같은 이름의 클래스가 있어 헷갈리기 쉽다.
 */
@Entity
@Table(name = "users")
public class AppUser {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(unique = true, nullable = false)
    private String username;

    /**
     * 비밀번호는 절대 원문으로 저장하지 않는다. 여기 담기는 것은 BCrypt 해시다.
     * BCrypt 는 같은 비밀번호라도 매번 다른 salt 를 섞으므로 해시값이 매번 달라지고,
     * 의도적으로 느리게 설계되어 있어 대량 대입 공격에 시간 비용을 강제한다.
     */
    @Column(nullable = false)
    private String password;

    /** "ROLE_USER" 또는 "ROLE_ADMIN". 스프링 시큐리티 관례상 ROLE_ 접두사를 붙인다. */
    @Column(nullable = false)
    private String role;

    protected AppUser() {
        // JPA 기본 생성자
    }

    public AppUser(String username, String encodedPassword, String role) {
        this.username = username;
        this.password = encodedPassword;
        this.role = role;
    }

    public Long getId() {
        return id;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getRole() {
        return role;
    }
}

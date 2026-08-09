package com.example.msa.product;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.math.BigDecimal;

@Entity
public class Product {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    // 금액은 부동소수점 오차가 없는 BigDecimal 로 다룬다.
    private BigDecimal price;

    private int stock;

    protected Product() {
        // JPA 가 리플렉션으로 인스턴스를 만들 때 사용하는 기본 생성자
    }

    public Product(String name, BigDecimal price, int stock) {
        this.name = name;
        this.price = price;
        this.stock = stock;
    }

    /**
     * 재고를 차감한다. 남은 수량보다 많이 요청되면 차감하지 않고 false 를 돌려준다.
     *
     * @return 차감에 성공했으면 true
     */
    public boolean decreaseStock(int quantity) {
        if (quantity > stock) {
            return false;
        }
        stock -= quantity;
        return true;
    }

    /**
     * 차감했던 재고를 되돌린다. Saga 의 <b>보상</b>에 쓰인다.
     *
     * <p>차감과 달리 상한을 검사하지 않는다. 되돌리는 수량은 앞서 실제로 차감한
     * 수량이므로 원래 값을 넘을 수 없고, 넘는다면 그것은 명령이 잘못된 것이지
     * 여기서 막을 일이 아니다. 중복 실행은 호출하는 쪽에서 멱등 기록으로 막는다.
     */
    public void increaseStock(int quantity) {
        stock += quantity;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public int getStock() {
        return stock;
    }
}

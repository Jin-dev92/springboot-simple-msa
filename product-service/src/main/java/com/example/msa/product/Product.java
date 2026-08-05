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

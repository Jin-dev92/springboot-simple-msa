package com.example.msa.order;

public enum OrderStatus {

    /** 주문은 받았지만 재고 확보 결과를 아직 듣지 못한 상태. */
    PENDING,

    /** 재고를 잡았다는 통보를 받아 확정된 상태. */
    CONFIRMED,

    /** 재고를 못 잡아 보상 처리로 취소된 상태. */
    CANCELLED
}

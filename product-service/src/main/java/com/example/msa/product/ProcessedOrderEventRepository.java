package com.example.msa.product;

import org.springframework.data.jpa.repository.JpaRepository;

interface ProcessedOrderEventRepository extends JpaRepository<ProcessedOrderEvent, Long> {
}

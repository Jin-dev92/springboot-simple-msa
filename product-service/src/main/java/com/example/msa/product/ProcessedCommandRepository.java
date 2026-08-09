package com.example.msa.product;

import org.springframework.data.jpa.repository.JpaRepository;

interface ProcessedCommandRepository extends JpaRepository<ProcessedCommand, String> {
}

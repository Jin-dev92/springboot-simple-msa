package com.example.msa.payment;

import org.springframework.data.jpa.repository.JpaRepository;

interface ProcessedCommandRepository extends JpaRepository<ProcessedCommand, String> {
}

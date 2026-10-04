package com.netflix.ads.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Table(name = "ad_campaigns")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdCampaign {

    @Id
    private String campaignId;

    private String advertiserName;

    private String targetDemographic;

    private BigDecimal remainingBudget;

    private BigDecimal targetCpm;

    private boolean active;

    private int currentImpressionCount;
}

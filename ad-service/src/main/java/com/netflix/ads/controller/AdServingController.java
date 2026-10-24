package com.netflix.ads.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.ads.model.AdCampaign;
import com.netflix.ads.repository.AdCampaignRepository;
import com.netflix.ads.service.KafkaEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/ads")
public class AdServingController {

    private final AdCampaignRepository campaignRepository;
    private final KafkaEventProducer kafkaEventProducer;
    private final ObjectMapper objectMapper;

    public AdServingController(AdCampaignRepository campaignRepository,
                               KafkaEventProducer kafkaEventProducer,
                               ObjectMapper objectMapper) {
        this.campaignRepository = campaignRepository;
        this.kafkaEventProducer = kafkaEventProducer;
        this.objectMapper = objectMapper;
    }

    /**
     * Serializes an operational event with Jackson so that user-controlled values
     * (campaign IDs, advertiser names, demographics) containing quotes, backslashes
     * or control characters are escaped and the Kafka payload is always valid JSON.
     */
    private String toEventJson(Map<String, Object> event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize operational event", e);
        }
    }

    @PostMapping("/campaigns")
    public ResponseEntity<AdCampaign> createCampaign(@RequestBody AdCampaign campaign) {
        AdCampaign saved = campaignRepository.save(campaign);
        return ResponseEntity.ok(saved);
    }

    @PostMapping("/serve")
    public ResponseEntity<?> serveAd(@RequestParam String campaignId, @RequestParam String userDemographic) {
        Optional<AdCampaign> campaignOpt = campaignRepository.findById(campaignId);

        if (campaignOpt.isEmpty()) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "CAMPAIGN_NOT_FOUND");
            event.put("campaignId", campaignId);
            event.put("userDemographic", userDemographic);
            event.put("timestamp", System.currentTimeMillis());
            String errorJson = toEventJson(event);
            kafkaEventProducer.emitOperationalEvent(campaignId, errorJson);
            return ResponseEntity.badRequest().body(Map.of("error", "Campaign not found", "campaignId", campaignId));
        }

        AdCampaign campaign = campaignOpt.get();

        // Check if campaign is inactive or budget exhausted
        if (!campaign.isActive() || campaign.getRemainingBudget().compareTo(BigDecimal.ZERO) <= 0) {
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("eventType", "AD_BUDGET_EXHAUSTED_ANOMALY");
            event.put("campaignId", campaign.getCampaignId());
            event.put("advertiser", campaign.getAdvertiserName());
            event.put("remainingBudget", campaign.getRemainingBudget());
            event.put("active", campaign.isActive());
            event.put("timestamp", System.currentTimeMillis());
            String errorJson = toEventJson(event);
            kafkaEventProducer.emitOperationalEvent(campaign.getCampaignId(), errorJson);

            return ResponseEntity.status(422).body(Map.of(
                    "error", "Ad serving rejected: Budget exhausted or campaign inactive",
                    "campaignId", campaignId
            ));
        }

        // Deduct impression cost ($0.05 per serve)
        campaign.setRemainingBudget(campaign.getRemainingBudget().subtract(new BigDecimal("0.05")));
        campaign.setCurrentImpressionCount(campaign.getCurrentImpressionCount() + 1);
        campaignRepository.save(campaign);

        return ResponseEntity.ok(Map.of(
                "status", "AD_SERVED_SUCCESSFULLY",
                "campaignId", campaign.getCampaignId(),
                "advertiser", campaign.getAdvertiserName(),
                "remainingBudget", campaign.getRemainingBudget()
        ));
    }
}

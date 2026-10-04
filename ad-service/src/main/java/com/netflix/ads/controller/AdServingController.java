package com.netflix.ads.controller;

import com.netflix.ads.model.AdCampaign;
import com.netflix.ads.repository.AdCampaignRepository;
import com.netflix.ads.service.KafkaEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/ads")
public class AdServingController {

    private final AdCampaignRepository campaignRepository;
    private final KafkaEventProducer kafkaEventProducer;

    public AdServingController(AdCampaignRepository campaignRepository, KafkaEventProducer kafkaEventProducer) {
        this.campaignRepository = campaignRepository;
        this.kafkaEventProducer = kafkaEventProducer;
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
            String errorJson = String.format(
                    "{\"eventType\": \"CAMPAIGN_NOT_FOUND\", \"campaignId\": \"%s\", \"userDemographic\": \"%s\", \"timestamp\": %d}",
                    campaignId, userDemographic, System.currentTimeMillis()
            );
            kafkaEventProducer.emitOperationalEvent(campaignId, errorJson);
            return ResponseEntity.badRequest().body(Map.of("error", "Campaign not found", "campaignId", campaignId));
        }

        AdCampaign campaign = campaignOpt.get();

        // Check if campaign is inactive or budget exhausted
        if (!campaign.isActive() || campaign.getRemainingBudget().compareTo(BigDecimal.ZERO) <= 0) {
            String errorJson = String.format(
                    "{\"eventType\": \"AD_BUDGET_EXHAUSTED_ANOMALY\", \"campaignId\": \"%s\", \"advertiser\": \"%s\", \"remainingBudget\": %s, \"active\": %b, \"timestamp\": %d}",
                    campaign.getCampaignId(), campaign.getAdvertiserName(), campaign.getRemainingBudget().toString(), campaign.isActive(), System.currentTimeMillis()
            );
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

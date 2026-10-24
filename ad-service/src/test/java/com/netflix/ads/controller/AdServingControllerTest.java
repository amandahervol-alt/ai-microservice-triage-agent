package com.netflix.ads.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.ads.model.AdCampaign;
import com.netflix.ads.repository.AdCampaignRepository;
import com.netflix.ads.service.KafkaEventProducer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AdServingController.class)
class AdServingControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private AdCampaignRepository campaignRepository;

    @MockBean
    private KafkaEventProducer kafkaEventProducer;

    private static AdCampaign campaign(String id, String advertiser, String budget, boolean active) {
        return new AdCampaign(id, advertiser, "US_ALL", new BigDecimal(budget), new BigDecimal("15.00"), active, 10);
    }

    private JsonNode captureEmittedEvent(String expectedKey) throws Exception {
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaEventProducer).emitOperationalEvent(eq(expectedKey), payload.capture());
        return objectMapper.readTree(payload.getValue()); // throws if payload is not valid JSON
    }

    @Test
    void serve_withBudget_servesAdAndDeductsCost() throws Exception {
        when(campaignRepository.findById("CMP-1")).thenReturn(Optional.of(campaign("CMP-1", "Acme", "10.00", true)));
        when(campaignRepository.save(any(AdCampaign.class))).thenAnswer(inv -> inv.getArgument(0));

        mockMvc.perform(post("/api/v1/ads/serve").param("campaignId", "CMP-1").param("userDemographic", "US_ALL"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("AD_SERVED_SUCCESSFULLY"))
                .andExpect(jsonPath("$.remainingBudget").value(9.95));

        ArgumentCaptor<AdCampaign> saved = ArgumentCaptor.forClass(AdCampaign.class);
        verify(campaignRepository).save(saved.capture());
        assertThat(saved.getValue().getCurrentImpressionCount()).isEqualTo(11);
        verify(kafkaEventProducer, never()).emitOperationalEvent(anyString(), anyString());
    }

    @Test
    void serve_unknownCampaign_returns400AndEmitsNotFoundEvent() throws Exception {
        when(campaignRepository.findById("MISSING")).thenReturn(Optional.empty());

        mockMvc.perform(post("/api/v1/ads/serve").param("campaignId", "MISSING").param("userDemographic", "US_ALL"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Campaign not found"));

        JsonNode event = captureEmittedEvent("MISSING");
        assertThat(event.get("eventType").asText()).isEqualTo("CAMPAIGN_NOT_FOUND");
        assertThat(event.get("userDemographic").asText()).isEqualTo("US_ALL");
        assertThat(event.get("timestamp").isNumber()).isTrue();
    }

    @Test
    void serve_exhaustedBudget_returns422AndEmitsAnomalyEvent() throws Exception {
        when(campaignRepository.findById("CMP-2")).thenReturn(Optional.of(campaign("CMP-2", "Acme", "0.00", true)));

        mockMvc.perform(post("/api/v1/ads/serve").param("campaignId", "CMP-2").param("userDemographic", "US_ALL"))
                .andExpect(status().isUnprocessableEntity());

        JsonNode event = captureEmittedEvent("CMP-2");
        assertThat(event.get("eventType").asText()).isEqualTo("AD_BUDGET_EXHAUSTED_ANOMALY");
        assertThat(event.get("advertiser").asText()).isEqualTo("Acme");
        assertThat(event.get("remainingBudget").decimalValue()).isEqualByComparingTo("0.00");
        assertThat(event.get("active").asBoolean()).isTrue();
        verify(campaignRepository, never()).save(any());
    }

    @Test
    void serve_inactiveCampaignWithBudget_returns422() throws Exception {
        when(campaignRepository.findById("CMP-3")).thenReturn(Optional.of(campaign("CMP-3", "Acme", "50.00", false)));

        mockMvc.perform(post("/api/v1/ads/serve").param("campaignId", "CMP-3").param("userDemographic", "US_ALL"))
                .andExpect(status().isUnprocessableEntity());

        assertThat(captureEmittedEvent("CMP-3").get("active").asBoolean()).isFalse();
    }

    @Test
    void anomalyEvent_isValidJson_whenAdvertiserNameContainsQuotesAndBackslashes() throws Exception {
        String nasty = "Bob's \"Best\" Ads \\ Co\nLine2";
        when(campaignRepository.findById("CMP-Q")).thenReturn(Optional.of(campaign("CMP-Q", nasty, "0.00", true)));

        mockMvc.perform(post("/api/v1/ads/serve").param("campaignId", "CMP-Q").param("userDemographic", "US_ALL"))
                .andExpect(status().isUnprocessableEntity());

        assertThat(captureEmittedEvent("CMP-Q").get("advertiser").asText()).isEqualTo(nasty);
    }

    @Test
    void notFoundEvent_isValidJson_whenRequestParamsContainQuotes() throws Exception {
        String id = "x\",\"injected\":\"1";
        when(campaignRepository.findById(id)).thenReturn(Optional.empty());

        mockMvc.perform(post("/api/v1/ads/serve").param("campaignId", id).param("userDemographic", "\"EU\""))
                .andExpect(status().isBadRequest());

        JsonNode event = captureEmittedEvent(id);
        assertThat(event.get("campaignId").asText()).isEqualTo(id);
        assertThat(event.has("injected")).isFalse();
        assertThat(event.get("userDemographic").asText()).isEqualTo("\"EU\"");
    }

    @Test
    void createCampaign_savesAndReturnsCampaign() throws Exception {
        when(campaignRepository.save(any(AdCampaign.class))).thenAnswer(inv -> inv.getArgument(0));

        mockMvc.perform(post("/api/v1/ads/campaigns")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"campaignId\":\"CMP-9\",\"advertiserName\":\"Acme\",\"targetDemographic\":\"US_ALL\","
                                + "\"remainingBudget\":100.00,\"targetCpm\":15.00,\"active\":true,\"currentImpressionCount\":0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.campaignId").value("CMP-9"))
                .andExpect(jsonPath("$.advertiserName").value("Acme"));
    }
}

package com.netflix.ads.repository;

import com.netflix.ads.model.AdCampaign;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AdCampaignRepository extends JpaRepository<AdCampaign, String> {
}

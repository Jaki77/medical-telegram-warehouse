{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH raw_detections AS (
    SELECT
        id,
        message_id,
        channel_name,
        image_path,
        detected_objects,
        confidence_scores,
        detection_count,
        image_category,
        processing_time,
        model_version,
        processed_at
    FROM {{ source('raw', 'image_detections') }}
),

enriched_detections AS (
    SELECT
        -- Create surrogate key
        {{ dbt_utils.surrogate_key(['rd.message_id', 'rd.channel_name']) }} as detection_key,
        
        -- Message information
        rd.message_id,
        rd.channel_name,
        
        -- Link to existing dimensions
        dc.channel_key,
        dd.date_key,
        
        -- Detection details
        rd.image_path,
        rd.detected_objects,
        rd.confidence_scores,
        rd.detection_count,
        rd.image_category,
        
        -- Confidence metrics
        CASE 
            WHEN rd.confidence_scores IS NOT NULL AND array_length(rd.confidence_scores, 1) > 0 
            THEN (SELECT AVG(unnest) FROM unnest(rd.confidence_scores))
            ELSE 0 
        END as avg_confidence,
        
        CASE 
            WHEN rd.confidence_scores IS NOT NULL AND array_length(rd.confidence_scores, 1) > 0 
            THEN (SELECT MAX(unnest) FROM unnest(rd.confidence_scores))
            ELSE 0 
        END as max_confidence,
        
        -- Object presence flags
        CASE 
            WHEN 'person' = ANY(rd.detected_objects) THEN TRUE 
            ELSE FALSE 
        END as has_person,
        
        CASE 
            WHEN 'bottle' = ANY(rd.detected_objects) OR 'cup' = ANY(rd.detected_objects) 
            OR 'bowl' = ANY(rd.detected_objects) OR 'vase' = ANY(rd.detected_objects) 
            THEN TRUE 
            ELSE FALSE 
        END as has_container,
        
        -- Processing metadata
        rd.processing_time,
        rd.model_version,
        rd.processed_at,
        
        -- Business insights
        CASE 
            WHEN rd.image_category = 'promotional' THEN 'High Engagement'
            WHEN rd.image_category = 'product_display' THEN 'Product Focused'
            WHEN rd.image_category = 'lifestyle' THEN 'Contextual'
            ELSE 'Other'
        END as content_strategy,
        
        -- Add metadata
        CURRENT_TIMESTAMP as loaded_at
        
    FROM raw_detections rd
    LEFT JOIN {{ ref('dim_channels') }} dc ON rd.channel_name = dc.channel_name
    LEFT JOIN {{ ref('dim_dates') }} dd ON DATE(rd.processed_at) = dd.full_date
),

final AS (
    SELECT
        detection_key,
        message_id,
        channel_key,
        date_key,
        image_path,
        detected_objects,
        confidence_scores,
        detection_count,
        image_category,
        avg_confidence,
        max_confidence,
        has_person,
        has_container,
        processing_time,
        model_version,
        processed_at,
        content_strategy,
        loaded_at,
        
        -- Derived metrics
        CASE 
            WHEN has_person AND has_container THEN 'Person with Product'
            WHEN has_person AND NOT has_container THEN 'Person Only'
            WHEN NOT has_person AND has_container THEN 'Product Only'
            ELSE 'Other'
        END as scene_composition,
        
        -- Quality flag
        CASE 
            WHEN detection_count = 0 THEN 'No Detection'
            WHEN avg_confidence < 0.3 THEN 'Low Confidence'
            WHEN avg_confidence >= 0.3 AND avg_confidence < 0.7 THEN 'Medium Confidence'
            ELSE 'High Confidence'
        END as detection_quality
        
    FROM enriched_detections
)

SELECT * FROM final
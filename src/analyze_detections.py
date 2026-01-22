"""
Analysis script for YOLO detection results
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from pathlib import Path
import json
from datetime import datetime

def analyze_detection_results():
    """Analyze YOLO detection results and answer business questions"""
    
    # Database connection
    engine = create_engine("postgresql://postgres:postgres123@localhost:5432/medical_warehouse")
    
    analysis_results = {
        "timestamp": datetime.now().isoformat(),
        "business_questions": {}
    }
    
    with engine.connect() as conn:
        # Question 1: Do "promotional" posts get more views than "product_display" posts?
        query1 = text("""
            SELECT 
                fid.image_category,
                AVG(fm.view_count) as avg_views,
                AVG(fm.forward_count) as avg_forwards,
                COUNT(*) as post_count
            FROM marts.fct_image_detections fid
            JOIN marts.fct_messages fm 
                ON fid.message_id = fm.message_id 
                AND fid.channel_name = (
                    SELECT channel_name 
                    FROM marts.dim_channels dc 
                    WHERE dc.channel_key = fm.channel_key
                )
            WHERE fid.image_category IN ('promotional', 'product_display')
            GROUP BY fid.image_category
            ORDER BY avg_views DESC
        """)
        
        result1 = pd.read_sql(query1, conn)
        analysis_results["business_questions"]["engagement_by_category"] = {
            "question": "Do 'promotional' posts get more views than 'product_display' posts?",
            "results": result1.to_dict('records'),
            "insight": "",
            "visualization": "engagement_by_category.png"
        }
        
        # Generate visualization for question 1
        if not result1.empty:
            plt.figure(figsize=(10, 6))
            bars = plt.bar(result1['image_category'], result1['avg_views'], color=['skyblue', 'lightcoral'])
            plt.title('Average Views by Image Category')
            plt.xlabel('Image Category')
            plt.ylabel('Average Views')
            plt.xticks(rotation=0)
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{height:.0f}', ha='center', va='bottom')
            
            plt.tight_layout()
            plt.savefig('data/processed/results/engagement_by_category.png', dpi=150)
            plt.close()
        
        # Question 2: Which channels use more visual content?
        query2 = text("""
            SELECT 
                dc.channel_name,
                dc.channel_type,
                COUNT(DISTINCT fid.detection_key) as total_detections,
                COUNT(DISTINCT CASE WHEN fid.has_person THEN fid.detection_key END) as person_images,
                COUNT(DISTINCT CASE WHEN fid.has_container THEN fid.detection_key END) as product_images,
                dc.image_percentage
            FROM marts.fct_image_detections fid
            JOIN marts.dim_channels dc ON fid.channel_key = dc.channel_key
            GROUP BY dc.channel_name, dc.channel_type, dc.image_percentage
            ORDER BY total_detections DESC
        """)
        
        result2 = pd.read_sql(query2, conn)
        analysis_results["business_questions"]["channel_visual_content"] = {
            "question": "Which channels use more visual content?",
            "results": result2.to_dict('records'),
            "insight": "",
            "visualization": "channel_visual_content.png"
        }
        
        # Generate visualization for question 2
        if not result2.empty:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            
            # Bar chart for total detections
            result2_sorted = result2.sort_values('total_detections', ascending=False)
            bars1 = ax1.barh(result2_sorted['channel_name'], result2_sorted['total_detections'], color='lightgreen')
            ax1.set_xlabel('Total Detections')
            ax1.set_title('Visual Content by Channel (Total Detections)')
            
            # Stacked bar for person vs product
            result2.set_index('channel_name')[['person_images', 'product_images']].plot(
                kind='barh', stacked=True, ax=ax2, color=['lightblue', 'lightcoral']
            )
            ax2.set_xlabel('Image Count')
            ax2.set_title('Person vs Product Images by Channel')
            ax2.legend(['Person Images', 'Product Images'])
            
            plt.tight_layout()
            plt.savefig('data/processed/results/channel_visual_content.png', dpi=150)
            plt.close()
        
        # Question 3: What are common object detections?
        query3 = text("""
            WITH object_counts AS (
                SELECT 
                    unnest(detected_objects) as object_name,
                    COUNT(*) as detection_count
                FROM marts.fct_image_detections
                WHERE detected_objects IS NOT NULL 
                    AND array_length(detected_objects, 1) > 0
                GROUP BY unnest(detected_objects)
            )
            SELECT 
                object_name,
                detection_count,
                ROUND(detection_count * 100.0 / SUM(detection_count) OVER (), 2) as percentage
            FROM object_counts
            ORDER BY detection_count DESC
            LIMIT 15
        """)
        
        result3 = pd.read_sql(query3, conn)
        analysis_results["business_questions"]["common_objects"] = {
            "question": "What are the most commonly detected objects?",
            "results": result3.to_dict('records'),
            "insight": "",
            "visualization": "common_objects.png"
        }
        
        # Generate visualization for question 3
        if not result3.empty:
            plt.figure(figsize=(12, 8))
            colors = plt.cm.Set3(range(len(result3)))
            bars = plt.bar(result3['object_name'], result3['detection_count'], color=colors)
            plt.title('Most Commonly Detected Objects')
            plt.xlabel('Object')
            plt.ylabel('Detection Count')
            plt.xticks(rotation=45, ha='right')
            
            # Add percentage labels
            for i, (bar, row) in enumerate(zip(bars, result3.itertuples())):
                plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.5,
                        f'{row.percentage}%', ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            plt.savefig('data/processed/results/common_objects.png', dpi=150)
            plt.close()
        
        # Question 4: Model limitations analysis
        query4 = text("""
            SELECT 
                image_category,
                detection_quality,
                COUNT(*) as count,
                AVG(avg_confidence) as avg_confidence_score
            FROM marts.fct_image_detections
            GROUP BY image_category, detection_quality
            ORDER BY image_category, detection_quality
        """)
        
        result4 = pd.read_sql(query4, conn)
        analysis_results["business_questions"]["model_limitations"] = {
            "question": "What are the limitations of using pre-trained models?",
            "results": result4.to_dict('records'),
            "insights": [
                "Pre-trained YOLOv8 detects general objects, not specific medical products",
                "Confidence scores vary based on object commonality in training data",
                "Medical-specific objects (pills, tablets) are not in standard classes",
                "Context understanding (promotional vs informational) is limited"
            ],
            "recommendations": [
                "Fine-tune model on medical product images",
                "Add custom classes for Ethiopian medical products",
                "Combine with text analysis for better context",
                "Implement human validation for critical decisions"
            ]
        }
        
        # Generate visualization for question 4
        if not result4.empty:
            pivot = result4.pivot(index='image_category', columns='detection_quality', values='count').fillna(0)
            plt.figure(figsize=(12, 8))
            pivot.plot(kind='bar', stacked=True, colormap='RdYlBu_r')
            plt.title('Detection Quality by Image Category')
            plt.xlabel('Image Category')
            plt.ylabel('Count')
            plt.legend(title='Detection Quality', bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('data/processed/results/detection_quality.png', dpi=150)
            plt.close()
    
    # Save analysis results
    results_dir = Path('data/processed/results')
    results_dir.mkdir(parents=True, exist_ok=True)
    
    report_path = results_dir / f"business_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(analysis_results, f, indent=2, default=str)
    
    # Generate summary report
    generate_summary_report(analysis_results, report_path)
    
    return analysis_results

def generate_summary_report(analysis_results: dict, report_path: Path):
    """Generate a human-readable summary report"""
    
    summary = []
    summary.append("="*70)
    summary.append("YOLO DETECTION ANALYSIS - BUSINESS INSIGHTS REPORT")
    summary.append("="*70)
    summary.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary.append("")
    
    for q_name, q_data in analysis_results["business_questions"].items():
        summary.append(f"QUESTION: {q_data['question']}")
        summary.append("-"*50)
        
        if 'results' in q_data and q_data['results']:
            if q_name == 'engagement_by_category':
                for result in q_data['results']:
                    summary.append(f"  {result['image_category']}:")
                    summary.append(f"    Average Views: {result['avg_views']:.0f}")
                    summary.append(f"    Average Forwards: {result['avg_forwards']:.0f}")
                    summary.append(f"    Post Count: {result['post_count']}")
                
                # Determine which performs better
                if len(q_data['results']) >= 2:
                    promo = next(r for r in q_data['results'] if r['image_category'] == 'promotional')
                    product = next(r for r in q_data['results'] if r['image_category'] == 'product_display')
                    
                    if promo['avg_views'] > product['avg_views']:
                        insight = "✓ PROMOTIONAL posts get MORE views than product display posts"
                    else:
                        insight = "✗ PROMOTIONAL posts get FEWER views than product display posts"
                    
                    summary.append("")
                    summary.append(f"INSIGHT: {insight}")
                    summary.append(f"  Difference: {abs(promo['avg_views'] - product['avg_views']):.0f} views")
            
            elif q_name == 'channel_visual_content':
                summary.append("Top 5 Channels by Visual Content:")
                for i, result in enumerate(q_data['results'][:5], 1):
                    summary.append(f"  {i}. {result['channel_name']} ({result['channel_type']})")
                    summary.append(f"     Total Detections: {result['total_detections']}")
                    summary.append(f"     Person Images: {result['person_images']}")
                    summary.append(f"     Product Images: {result['product_images']}")
            
            elif q_name == 'common_objects':
                summary.append("Top 10 Detected Objects:")
                for i, result in enumerate(q_data['results'][:10], 1):
                    summary.append(f"  {i}. {result['object_name']}: {result['detection_count']} ({result['percentage']}%)")
            
            elif q_name == 'model_limitations':
                summary.append("Key Limitations Identified:")
                for insight in q_data.get('insights', []):
                    summary.append(f"  • {insight}")
                
                summary.append("")
                summary.append("Recommendations:")
                for rec in q_data.get('recommendations', []):
                    summary.append(f"  • {rec}")
        
        summary.append("")
        summary.append(f"Visualization saved: {q_data.get('visualization', 'N/A')}")
        summary.append("")
    
    summary.append("="*70)
    summary.append("CONCLUSIONS:")
    summary.append("="*70)
    
    # Extract key conclusions
    engagement_data = analysis_results["business_questions"]["engagement_by_category"]["results"]
    if engagement_data:
        promo_views = next(r['avg_views'] for r in engagement_data if r['image_category'] == 'promotional')
        product_views = next(r['avg_views'] for r in engagement_data if r['image_category'] == 'product_display')
        
        if promo_views > product_views:
            summary.append("1. Promotional content (people with products) generates higher engagement")
        else:
            summary.append("1. Product-focused content generates higher engagement")
    
    visual_data = analysis_results["business_questions"]["channel_visual_content"]["results"]
    if visual_data:
        top_channel = visual_data[0]['channel_name']
        summary.append(f"2. {top_channel} uses the most visual content")
    
    object_data = analysis_results["business_questions"]["common_objects"]["results"]
    if object_data:
        top_object = object_data[0]['object_name']
        summary.append(f"3. Most common detected object: {top_object}")
    
    summary.append("4. Pre-trained YOLO provides valuable insights but needs domain adaptation")
    summary.append("5. Combined text+image analysis will provide most accurate insights")
    
    summary.append("")
    summary.append(f"Full analysis saved to: {report_path}")
    summary.append("="*70)
    
    # Save summary to file
    summary_path = report_path.parent / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(summary_path, 'w') as f:
        f.write('\n'.join(summary))
    
    # Print to console
    print('\n'.join(summary))
    
    return summary_path

if __name__ == "__main__":
    print("Starting YOLO detection analysis...")
    results = analyze_detection_results()
    print("\nAnalysis complete!")
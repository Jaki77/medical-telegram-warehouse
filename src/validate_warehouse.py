import pandas as pd
from sqlalchemy import create_engine, text
import json
from pathlib import Path

def validate_star_schema():
    """Validate the star schema implementation"""
    
    # Database connection
    engine = create_engine("postgresql://postgres:postgres123@localhost:5432/medical_warehouse")
    
    validation_results = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "schema_validation": {},
        "data_quality": {}
    }
    
    with engine.connect() as conn:
        # Check if all tables exist
        tables = ['staging.stg_telegram_messages', 'marts.dim_channels', 'marts.dim_dates', 'marts.fct_messages']
        
        for table in tables:
            try:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                validation_results["schema_validation"][table] = {
                    "exists": True,
                    "row_count": count
                }
            except Exception as e:
                validation_results["schema_validation"][table] = {
                    "exists": False,
                    "error": str(e)
                }
        
        # Check referential integrity
        try:
            # Check foreign key relationships
            fk_check = conn.execute(text("""
                SELECT 
                    COUNT(*) as orphaned_messages
                FROM marts.fct_messages fm
                LEFT JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
                WHERE dc.channel_key IS NULL
            """))
            orphaned = fk_check.scalar()
            validation_results["data_quality"]["referential_integrity"] = {
                "orphaned_messages": orphaned,
                "status": "PASS" if orphaned == 0 else "FAIL"
            }
        except Exception as e:
            validation_results["data_quality"]["referential_integrity"] = {
                "error": str(e),
                "status": "ERROR"
            }
        
        # Check data freshness
        try:
            freshness = conn.execute(text("""
                SELECT 
                    MAX(message_date) as latest_message,
                    CURRENT_TIMESTAMP as current_time,
                    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(message_date)))/3600 as hours_behind
                FROM staging.stg_telegram_messages
            """))
            row = freshness.fetchone()
            validation_results["data_quality"]["freshness"] = {
                "latest_message": str(row[0]),
                "hours_behind": row[2],
                "status": "PASS" if row[2] < 24 else "WARNING"
            }
        except Exception as e:
            validation_results["data_quality"]["freshness"] = {
                "error": str(e),
                "status": "ERROR"
            }
    
    # Save validation results
    output_path = Path("logs") / "warehouse_validation.json"
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(validation_results, f, indent=2, default=str)
    
    print("\n" + "="*50)
    print("WAREHOUSE VALIDATION RESULTS")
    print("="*50)
    
    # Print summary
    for category, results in validation_results.items():
        if category != "timestamp":
            print(f"\n{category.upper()}:")
            for key, value in results.items():
                if isinstance(value, dict):
                    print(f"  {key}:")
                    for subkey, subvalue in value.items():
                        print(f"    {subkey}: {subvalue}")
                else:
                    print(f"  {key}: {value}")
    
    print(f"\nFull results saved to: {output_path}")
    print("="*50)
    
    return validation_results

if __name__ == "__main__":
    validate_star_schema()
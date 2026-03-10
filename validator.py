import pandas as pd
import logging
from pydantic import BaseModel, Field, field_validator, ValidationError
from datetime import datetime
from typing import List, Optional

# Custom 'Detective' Logger
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataIntegrityDetective")

class SupplyChainRecord(BaseModel):
    """
    Canonical Schema for Supply Chain Transactions.
    Ensures OTIF/OOS metrics are derived from high-integrity data.
    """
    transaction_id: str = Field(description="Unique ID for the event")
    product_id: str
    quantity: int = Field(gt=-1, description="Quantity cannot be negative")
    warehouse_id: str
    status: str
    timestamp: datetime

    @field_validator('status')
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {'Delivered', 'In Transit', 'Pending', 'Cancelled'}
        if v not in allowed:
            raise ValueError(f"Invalid status: {v}. Must be one of {allowed}")
        return v

class IntegrityGuard:
    def __init__(self):
        self.anomaly_log = []

    def audit_dataframe(self, df: pd.DataFrame):
        """
        Iterates through records to find 'Reporting Anomalies'.
        """
        logger.info(f"🕵️ Analyzing {len(df)} records for integrity gaps...")
        
        records = df.to_dict(orient='records')
        clean_records = []
        
        for idx, row in enumerate(records):
            try:
                # Validate against the Pydantic model
                valid_record = SupplyChainRecord(**row)
                clean_records.append(valid_record.model_dump())
            except ValidationError as e:
                # Capture the specific anomaly for triage 
                error_msg = f"Row {idx} | ID: {row.get('transaction_id')} | Error: {e.json()}"
                self.anomaly_log.append(error_msg)
                logger.warning(f"⚠️ Anomaly Detected: {error_msg}")

        self._report_findings(len(records))
        return pd.DataFrame(clean_records)

    def _report_findings(self, total):
        anomaly_count = len(self.anomaly_log)
        accuracy_rate = ((total - anomaly_count) / total) * 100
        
        logger.info("-" * 30)
        logger.info(f"Audit Complete. Integrity Score: {accuracy_rate:.2f}%")
        if anomaly_count > 0:
            logger.error(f"Found {anomaly_count} anomalies. Alerting CloudWatch...")
            # Simulation of the CloudWatch alert integration 
        else:
            logger.info("✅ All records verified. Data is 'Safe' for production.")

if __name__ == "__main__":
    # Mock messy data simulating real-world 'Choke Points' 
    messy_data = pd.DataFrame([
        {"transaction_id": "TXN_001", "product_id": "A1", "quantity": 50, "warehouse_id": "SEA_1", "status": "Delivered", "timestamp": datetime.now()},
        {"transaction_id": "TXN_002", "product_id": "B2", "quantity": -10, "warehouse_id": "GUJ_2", "status": "Pending", "timestamp": datetime.now()}, # Error: Neg Qty
        {"transaction_id": "TXN_003", "product_id": "C3", "quantity": 100, "warehouse_id": "BEL_1", "status": "Broken", "timestamp": datetime.now()}   # Error: Bad Status
    ])

    guard = IntegrityGuard()
    validated_df = guard.audit_dataframe(messy_data)

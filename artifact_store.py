"""
Artifact Store for Cognix V2.

Persists analytics results as parquet files for reuse and audit.
Provides content-addressable storage with MD5 hashing.
"""

import os
import hashlib
from typing import Optional
from datetime import datetime
import pandas as pd
from loguru import logger

from config import settings


class ArtifactStore:
    """
    Stores and retrieves analytics artifacts.
    
    Features:
    - Content-addressable storage (same query = same artifact)
    - Parquet format for efficient storage and retrieval
    - Metadata tracking
    - Artifact expiration (future)
    """
    
    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = base_dir or str(settings.artifacts_dir)
        os.makedirs(self.base_dir, exist_ok=True)
        logger.info(f"ArtifactStore initialized at: {self.base_dir}")
    
    def _generate_id(self, df: pd.DataFrame) -> str:
        """Generate unique ID based on DataFrame content."""
        content = df.to_csv(index=False)
        return hashlib.md5(content.encode()).hexdigest()
    
    def save(self, df: pd.DataFrame, metadata: Optional[dict] = None) -> str:
        """
        Save a DataFrame as an artifact.
        
        Args:
            df: DataFrame to save
            metadata: Optional metadata to associate with artifact
            
        Returns:
            Path to saved artifact
        """
        artifact_id = self._generate_id(df)
        path = os.path.join(self.base_dir, f"{artifact_id}.parquet")
        
        # Save the DataFrame
        df.to_parquet(path, index=False)
        
        # Save metadata if provided
        if metadata:
            metadata_path = os.path.join(self.base_dir, f"{artifact_id}.meta.json")
            import json
            metadata["created_at"] = datetime.now().isoformat()
            metadata["row_count"] = len(df)
            metadata["columns"] = list(df.columns)
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Saved artifact: {path} ({len(df)} rows)")
        return path
    
    def load(self, artifact_id: str) -> Optional[pd.DataFrame]:
        """
        Load an artifact by ID.
        
        Args:
            artifact_id: The artifact ID (MD5 hash)
            
        Returns:
            DataFrame or None if not found
        """
        path = os.path.join(self.base_dir, f"{artifact_id}.parquet")
        
        if not os.path.exists(path):
            logger.warning(f"Artifact not found: {artifact_id}")
            return None
        
        df = pd.read_parquet(path)
        logger.info(f"Loaded artifact: {artifact_id} ({len(df)} rows)")
        return df
    
    def list_artifacts(self) -> list[dict]:
        """List all stored artifacts with metadata."""
        artifacts = []
        
        for filename in os.listdir(self.base_dir):
            if filename.endswith(".parquet"):
                artifact_id = filename.replace(".parquet", "")
                path = os.path.join(self.base_dir, filename)
                
                # Get file stats
                stat = os.stat(path)
                
                artifacts.append({
                    "id": artifact_id,
                    "path": path,
                    "size_bytes": stat.st_size,
                    "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat()
                })
        
        return sorted(artifacts, key=lambda x: x["created_at"], reverse=True)
    
    def delete(self, artifact_id: str) -> bool:
        """Delete an artifact by ID."""
        path = os.path.join(self.base_dir, f"{artifact_id}.parquet")
        meta_path = os.path.join(self.base_dir, f"{artifact_id}.meta.json")
        
        deleted = False
        
        if os.path.exists(path):
            os.remove(path)
            deleted = True
            logger.info(f"Deleted artifact: {artifact_id}")
        
        if os.path.exists(meta_path):
            os.remove(meta_path)
        
        return deleted
    
    def cleanup_old(self, max_age_days: int = 7) -> int:
        """Remove artifacts older than max_age_days."""
        import time
        
        cutoff = time.time() - (max_age_days * 24 * 60 * 60)
        deleted_count = 0
        
        for filename in os.listdir(self.base_dir):
            if filename.endswith(".parquet"):
                path = os.path.join(self.base_dir, filename)
                if os.stat(path).st_ctime < cutoff:
                    artifact_id = filename.replace(".parquet", "")
                    self.delete(artifact_id)
                    deleted_count += 1
        
        logger.info(f"Cleaned up {deleted_count} old artifacts")
        return deleted_count


# Singleton instance
_store: Optional[ArtifactStore] = None


def get_artifact_store() -> ArtifactStore:
    """Get or create the artifact store singleton."""
    global _store
    if _store is None:
        _store = ArtifactStore()
    return _store


def save_artifact(df: pd.DataFrame, metadata: Optional[dict] = None) -> str:
    """Convenience function to save an artifact."""
    store = get_artifact_store()
    return store.save(df, metadata)


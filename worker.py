import os
import json
import time
import tempfile
import traceback
import threading
import asyncio
import gc
from datetime import datetime, timezone
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor

# Google Cloud imports
from google.cloud import storage, pubsub_v1, firestore
from google.api_core import exceptions as gcp_exceptions

# Import ULTRA-FAST PDF processor
from ultra_fast_pdf_processor import UltraFastPDFProcessor

# FastAPI untuk health check endpoint
from fastapi import FastAPI
import uvicorn


class UltraFastDocumentWorker:
    """
    ULTRA-OPTIMIZED Worker untuk production
    Target: 12.6 menit → 3-4 menit total processing time
    """
    
    def __init__(self):
        """Initialize ULTRA-FAST worker dengan optimized settings"""

        # Environment variables
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "bni-prod-dma-bnimove-ai")
        self.subscription_name = os.getenv("PUBSUB_SUBSCRIPTION", "document-processing-worker")
        self.results_topic = os.getenv("PUBSUB_RESULTS_TOPIC", "document-processing-results")
        self.firestore_database = os.getenv("FIRESTORE_DATABASE", "document-processing-firestore")
        
        # ULTRA-OPTIMIZED: Single worker untuk better resource utilization
        self.max_workers = int(os.getenv("MAX_WORKERS", "1"))
        self.port = int(os.getenv("PORT", "8080"))

        # ULTRA-FAST processor configuration
        self.processor_config = {
            "api_key": os.getenv("OPENWEBUI_API_KEY", "dummy-api-key"),
            "base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:8080"),
            "model": os.getenv("OPENWEBUI_MODEL", "image-screening-shmshm-elektronik"),
            
            # BREAKTHROUGH OPTIMIZATIONS
            "min_delay_between_requests": int(os.getenv("MIN_DELAY_SECONDS", "15")),  # 70→15 (5x faster!)
            "safety_margin": int(os.getenv("SAFETY_MARGIN", "1")),  # 5→1
            "timeout_seconds": int(os.getenv("TIMEOUT_SECONDS", "90")),  # 150→90
            "chunk_size": int(os.getenv("CHUNK_SIZE", "4")),  # 2→4 (fewer API calls)
            "max_image_size_kb": int(os.getenv("MAX_IMAGE_SIZE_KB", "300")),  # 400→300
            "base_image_quality": int(os.getenv("BASE_IMAGE_QUALITY", "80")),  # 88→80
        }

        # Initialize GCP clients with connection pooling
        self.storage_client = storage.Client(project=self.project_id)
        self.subscriber = pubsub_v1.SubscriberClient()
        self.publisher = pubsub_v1.PublisherClient()
        self.firestore_client = firestore.Client(
            project=self.project_id,
            database=self.firestore_database,
        )

        # Connection paths
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_name
        )
        self.results_topic_path = self.publisher.topic_path(
            self.project_id, self.results_topic
        )

        # Worker status tracking
        self.is_running = False
        self.last_heartbeat = datetime.now(timezone.utc)
        self.processed_jobs = 0
        self.failed_jobs = 0

        # OPTIMIZATION: Processor instance reuse untuk avoid initialization overhead
        self.cached_processor = None
        self.last_processor_config = None

        # OPTIMIZATION: Thread pool untuk async operations
        self.io_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="FastIO")

        print(f"🚀 ULTRA-FAST Worker initialized:")
        print(f"   Project: {self.project_id}")
        print(f"   Subscription: {self.subscription_name}")
        print(f"   Max workers: {self.max_workers}")
        print(f"   🔥 ULTRA-OPTIMIZED settings:")
        print(f"      - Delays reduced: 70s → {self.processor_config['min_delay_between_requests']}s")
        print(f"      - Chunk size: 2 → {self.processor_config['chunk_size']} pages")
        print(f"      - Image quality: 88 → {self.processor_config['base_image_quality']}%")
        print(f"      - Timeout: 150s → {self.processor_config['timeout_seconds']}s")

    def get_ultra_fast_processor(self, document_type: str, model_name: str = None) -> UltraFastPDFProcessor:
        """
        Get cached processor instance untuk avoid initialization overhead
        """
        config = self.processor_config.copy()
        config["document_type"] = document_type
        if model_name:
            config["model"] = model_name

        # Create cache key
        config_key = f"{config['model']}_{document_type}_{config['min_delay_between_requests']}"
        
        # Reuse processor if configuration hasn't changed
        if (self.cached_processor is not None and 
            self.last_processor_config == config_key):
            print("🔄 Reusing cached processor instance")
            return self.cached_processor
        
        # Create new processor
        print("⚡ Creating new ULTRA-FAST processor")
        self.cached_processor = UltraFastPDFProcessor(config)
        self.cached_processor.enable_logging = True
        self.last_processor_config = config_key
        
        return self.cached_processor

    def ultra_fast_download_from_gcs(self, gcs_path: str, local_path: str) -> bool:
        """
        ULTRA-FAST GCS download dengan streaming dan timeout
        """
        try:
            download_start = time.time()
            
            if not gcs_path.startswith("gs://"):
                raise ValueError(f"Invalid GCS path: {gcs_path}")

            path_parts = gcs_path[5:].split("/", 1)
            bucket_name = path_parts[0]
            blob_name = path_parts[1]

            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # ULTRA-FAST: Stream download dengan aggressive timeout
            with open(local_path, 'wb') as f:
                blob.download_to_file(f, timeout=20)  # 20 second timeout
            
            download_time = time.time() - download_start
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            speed_mbps = file_size_mb / download_time if download_time > 0 else 0
            
            print(f"⚡ ULTRA-FAST download: {download_time:.2f}s, {file_size_mb:.1f}MB ({speed_mbps:.1f} MB/s)")
            return True

        except Exception as e:
            print(f"❌ Download failed: {e}")
            return False

    def ultra_fast_update_job_status(self, job_id: str, status: str, result: Dict = None, error: str = None):
        """
        ULTRA-FAST Firestore update dengan aggressive timeout
        """
        try:
            start_time = time.time()
            
            doc_ref = self.firestore_client.collection("jobs").document(job_id)

            update_data = {
                "status": status,
                "updated_at": datetime.now(timezone.utc),
            }

            if status == "completed":
                update_data["completed_at"] = datetime.now(timezone.utc)
                if result:
                    update_data["result"] = result
                self.processed_jobs += 1
            elif status == "failed":
                update_data["completed_at"] = datetime.now(timezone.utc)
                if error:
                    update_data["error"] = error
                self.failed_jobs += 1

            # ULTRA-FAST: Short timeout
            doc_ref.update(update_data, timeout=5.0)
            
            update_time = time.time() - start_time
            print(f"⚡ Status updated in {update_time:.2f}s: {job_id} → {status}")

        except Exception as e:
            print(f"❌ Firestore update failed: {e}")

    def ultra_fast_publish_result(self, job_id: str, result: Dict, status: str):
        """
        ULTRA-FAST result publishing
        """
        try:
            start_time = time.time()
            
            message_data = {
                "job_id": job_id,
                "status": status,
                "result": result,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

            message_json = json.dumps(message_data).encode("utf-8")
            
            # ULTRA-FAST: Short timeout
            future = self.publisher.publish(self.results_topic_path, message_json)
            message_id = future.result(timeout=5.0)

            publish_time = time.time() - start_time
            print(f"⚡ Result published in {publish_time:.2f}s: {message_id}")

        except Exception as e:
            print(f"❌ Publish failed: {e}")

    def ultra_fast_process_document(self, job_data: Dict) -> Dict:
        """
        ULTRA-FAST document processing dengan minimal overhead
        """
        job_id = job_data["job_id"]
        document_type = job_data["document_type"]
        gcs_path = job_data["gcs_path"]
        filename = job_data["filename"]
        model_name = job_data.get("model_name")

        print(f"🚀 ULTRA-FAST processing: {job_id} ({document_type} - {filename})")
        total_start = time.time()

        try:
            # STEP 1: ULTRA-FAST temp file creation
            setup_start = time.time()
            with tempfile.NamedTemporaryFile(
                suffix=os.path.splitext(filename)[1], delete=False
            ) as tmp_file:
                tmp_path = tmp_file.name
            setup_time = time.time() - setup_start

            try:
                # STEP 2: ULTRA-FAST download
                download_start = time.time()
                if not self.ultra_fast_download_from_gcs(gcs_path, tmp_path):
                    raise Exception("Download failed")
                download_time = time.time() - download_start

                # STEP 3: Get cached processor (ULTRA-FAST initialization)
                processor_start = time.time()
                processor = self.get_ultra_fast_processor(document_type, model_name)
                processor_init_time = time.time() - processor_start

                # STEP 4: ULTRA-FAST processing
                processing_start = time.time()
                print(f"⚡ Processing with ULTRA-FAST settings:")
                print(f"   - Model: {processor.model}")
                print(f"   - Delays: {processor.min_delay_between_requests}s")
                print(f"   - Chunks: {processor.chunk_size} pages")
                print(f"   - Quality: {processor.base_image_quality}%")

                result = processor.process_file(tmp_path)
                processing_time = time.time() - processing_start

                total_time = time.time() - total_start
                overhead_time = total_time - processing_time

                print(f"🎉 ULTRA-FAST processing completed:")
                print(f"   ⏱️ Breakdown:")
                print(f"      - Setup: {setup_time:.2f}s")
                print(f"      - Download: {download_time:.2f}s") 
                print(f"      - Processor init: {processor_init_time:.2f}s")
                print(f"      - Processing: {processing_time:.2f}s")
                print(f"   📊 Performance:")
                print(f"      - Total time: {total_time:.2f}s ({total_time/60:.1f} minutes)")
                print(f"      - Overhead: {overhead_time:.2f}s ({overhead_time/total_time*100:.1f}%)")
                print(f"   🚀 Speedup: ~{(12.6*60)/total_time:.1f}x faster than baseline!")

                return {
                    "success": True,
                    "result": result,
                    "performance_metrics": {
                        "setup_time": round(setup_time, 2),
                        "download_time": round(download_time, 2),
                        "processor_init_time": round(processor_init_time, 2),
                        "processing_time": round(processing_time, 2),
                        "total_time": round(total_time, 2),
                        "overhead_time": round(overhead_time, 2),
                        "overhead_percentage": round(overhead_time/total_time*100, 1),
                        "speedup_factor": round((12.6*60)/total_time, 1)
                    },
                    "model_used": processor.model,
                    "document_type": document_type,
                    "ultra_fast_optimization": True,
                }

            finally:
                # ULTRA-FAST cleanup
                try:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                except Exception:
                    pass
                # Force garbage collection
                gc.collect()

        except Exception as e:
            error_time = time.time() - total_start
            error_msg = f"ULTRA-FAST processing failed: {str(e)}"
            print(f"❌ {error_msg} (after {error_time:.2f}s)")

            return {
                "success": False,
                "error": error_msg,
                "processing_time": round(error_time, 2),
                "ultra_fast_optimization": True,
                "traceback": traceback.format_exc(),
            }

    def process_single_message(self, message_data: Dict, ack_id: str):
        """
        ULTRA-FAST single message processing
        """
        job_id = None
        message_start_time = time.time()

        try:
            # ULTRA-FAST validation
            if not isinstance(message_data, dict):
                print(f"❌ Invalid message format")
                self.subscriber.modify_ack_deadline(
                    subscription=self.subscription_path,
                    ack_ids=[ack_id],
                    ack_deadline_seconds=0,
                )
                return

            job_id = message_data.get("job_id", "unknown")
            print(f"⚡ ULTRA-FAST processing message: {job_id}")

            # Quick validation
            required_fields = ["job_id", "document_type", "gcs_path", "filename"]
            missing_fields = [field for field in required_fields if not message_data.get(field)]
            
            if missing_fields:
                error_msg = f"Missing fields: {missing_fields}"
                print(f"❌ {error_msg}")
                
                if job_id != "unknown":
                    self.ultra_fast_update_job_status(job_id, "failed", error=error_msg)
                
                self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[ack_id])
                return

            # ULTRA-FAST job status check
            try:
                doc_ref = self.firestore_client.collection("jobs").document(job_id)
                job_doc = doc_ref.get(timeout=2.0)  # Very quick timeout
                
                if job_doc.exists:
                    current_status = job_doc.to_dict().get("status", "unknown")
                    if current_status in ["completed", "failed"]:
                        print(f"⚠️ Job {job_id} already {current_status}, skipping")
                        self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[ack_id])
                        return
            except Exception:
                # Continue if status check fails
                pass

            # Mark as processing
            self.ultra_fast_update_job_status(job_id, "processing")

            # ULTRA-FAST processing
            try:
                result = self.ultra_fast_process_document(message_data)
                
                message_time = time.time() - message_start_time
                print(f"📊 ULTRA-FAST total message processing: {message_time:.2f}s")

                if result["success"]:
                    # SUCCESS path
                    self.ultra_fast_update_job_status(job_id, "completed", result=result["result"])
                    self.ultra_fast_publish_result(job_id, result["result"], "completed")
                    
                    print(f"✅ Job {job_id} completed successfully")
                    
                    # ACK message
                    self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[ack_id])
                else:
                    # FAILURE path
                    self.ultra_fast_update_job_status(job_id, "failed", error=result["error"])
                    self.ultra_fast_publish_result(job_id, {"error": result["error"]}, "failed")
                    
                    print(f"❌ Job {job_id} failed: {result['error']}")
                    
                    # ACK failed job
                    self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[ack_id])

            except Exception as processing_error:
                error_msg = f"Processing exception: {str(processing_error)}"
                print(f"❌ {error_msg}")
                
                self.ultra_fast_update_job_status(job_id, "failed", error=error_msg)
                self.subscriber.acknowledge(subscription=self.subscription_path, ack_ids=[ack_id])

        except Exception as e:
            error_msg = f"Message error: {str(e)}"
            print(f"❌ {error_msg}")
            
            if job_id and job_id != "unknown":
                try:
                    self.ultra_fast_update_job_status(job_id, "failed", error=error_msg)
                except Exception:
                    pass
            
            # NACK untuk retry
            try:
                self.subscriber.modify_ack_deadline(
                    subscription=self.subscription_path,
                    ack_ids=[ack_id],
                    ack_deadline_seconds=0,
                )
            except Exception:
                pass

    def start_ultra_fast_polling_worker(self):
        """
        Start ULTRA-FAST polling worker
        """
        print(f"🚀 Starting ULTRA-FAST polling worker...")
        print(f"Subscription: {self.subscription_path}")

        self.is_running = True
        consecutive_empty = 0
        max_empty_polls = 12

        try:
            while self.is_running:
                try:
                    # Heartbeat
                    self.last_heartbeat = datetime.now(timezone.utc)

                    # ULTRA-FAST message pulling
                    response = self.subscriber.pull(
                        subscription=self.subscription_path,
                        max_messages=self.max_workers,
                        timeout=10.0,  # Short timeout
                    )

                    if response.received_messages:
                        consecutive_empty = 0
                        print(f"📨 Received {len(response.received_messages)} messages (ULTRA-FAST mode)")

                        # Process messages
                        for received_message in response.received_messages:
                            try:
                                raw_data = received_message.message.data

                                if not raw_data:
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                    continue

                                # ULTRA-FAST JSON parsing
                                try:
                                    decoded_data = raw_data.decode("utf-8")
                                    message_data = json.loads(decoded_data)
                                except (UnicodeDecodeError, json.JSONDecodeError):
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                    continue

                                # Process with ULTRA-FAST method
                                self.process_single_message(
                                    message_data, received_message.ack_id
                                )

                                # ULTRA-FAST cleanup
                                gc.collect()

                            except Exception as e:
                                print(f"❌ Message error: {e}")
                                try:
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                except Exception:
                                    pass
                    else:
                        consecutive_empty += 1
                        if consecutive_empty == 1:
                            print(f"📭 No messages (ULTRA-FAST worker ready)")
                        elif consecutive_empty >= max_empty_polls:
                            print(f"💓 ULTRA-FAST heartbeat - active and optimized...")
                            consecutive_empty = 0

                        time.sleep(3)  # Short sleep

                except KeyboardInterrupt:
                    print("\n🛑 ULTRA-FAST worker shutdown requested")
                    break
                except Exception as e:
                    print(f"❌ ULTRA-FAST polling error: {e}")
                    time.sleep(5)  # Quick retry

        except Exception as e:
            print(f"❌ ULTRA-FAST worker failed: {e}")
            raise
        finally:
            self.is_running = False
            # Cleanup thread pool
            self.io_executor.shutdown(wait=True)

        print("✅ ULTRA-FAST worker shutdown complete")

    def get_health_status(self):
        """Get ULTRA-FAST worker health status"""
        now = datetime.now(timezone.utc)
        time_since_heartbeat = (now - self.last_heartbeat).total_seconds()

        return {
            "status": "healthy" if self.is_running and time_since_heartbeat < 60 else "unhealthy",
            "is_running": self.is_running,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "time_since_heartbeat": round(time_since_heartbeat, 2),
            "processed_jobs": self.processed_jobs,
            "failed_jobs": self.failed_jobs,
            "success_rate": round(
                self.processed_jobs / max(1, self.processed_jobs + self.failed_jobs) * 100, 2,
            ),
            "subscription": self.subscription_name,
            "project_id": self.project_id,
            "optimization_version": "ULTRA-FAST v3.0",
            "ultra_optimizations": {
                "delay_reduction": "70s → 15s (5x faster)",
                "chunk_size_optimized": "2 → 4 pages",
                "image_quality_optimized": "88% → 80%",
                "timeout_reduced": "150s → 90s",
                "processor_caching": "enabled",
                "aggressive_gc": "enabled",
                "fast_downloads": "enabled",
                "target_speedup": "5-10x faster overall"
            }
        }


# Global worker instance
worker = None

# FastAPI app
app = FastAPI(title="ULTRA-FAST Document Processing Worker", version="3.0.0")

@app.get("/")
async def root():
    return {
        "service": "ULTRA-FAST Document Processing Worker",
        "status": "running",
        "version": "3.0.0",
        "optimizations": "ULTRA-FAST processing with 5-10x speedup",
        "features": [
            "15s delays (was 70s)",
            "4-page chunks (was 2)",
            "Processor caching",
            "Aggressive optimizations",
            "Production ready"
        ]
    }

@app.get("/health")
async def health():
    global worker
    if worker:
        return worker.get_health_status()
    else:
        return {"status": "initializing", "message": "ULTRA-FAST Worker initializing..."}

@app.get("/metrics")
async def metrics():
    global worker
    if worker:
        status = worker.get_health_status()
        return {
            "processed_jobs": status["processed_jobs"],
            "failed_jobs": status["failed_jobs"],
            "success_rate": status["success_rate"],
            "uptime_seconds": status["time_since_heartbeat"],
            "optimization_version": status["optimization_version"],
            "ultra_optimizations": status["ultra_optimizations"]
        }
    else:
        return {"message": "ULTRA-FAST Worker not initialized"}

def main():
    """Main entry point untuk ULTRA-FAST worker"""
    global worker

    try:
        print("🚀 Initializing ULTRA-FAST Document Processing Worker...")

        # Initialize worker
        worker = UltraFastDocumentWorker()

        # Start worker thread
        worker_thread = threading.Thread(
            target=worker.start_ultra_fast_polling_worker, daemon=True
        )
        worker_thread.start()

        print(f"✅ ULTRA-FAST Worker thread started")
        print(f"🌐 Starting HTTP server on port {worker.port}")
        print(f"🎯 Target performance: 3-4 minutes (was 12.6 minutes)")

        # Start HTTP server
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=worker.port,
            log_level="info",
        )

    except KeyboardInterrupt:
        print("\n🛑 ULTRA-FAST Worker stopped by user")
        if worker:
            worker.is_running = False
    except Exception as e:
        print(f"❌ ULTRA-FAST Worker failed to start: {e}")
        print(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    main()

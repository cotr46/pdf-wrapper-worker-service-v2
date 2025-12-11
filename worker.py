import os
import json
import time
import tempfile
import traceback
import threading
from datetime import datetime, timezone
from typing import Dict, Any

# Google Cloud imports
from google.cloud import storage, pubsub_v1, firestore
from google.api_core import exceptions as gcp_exceptions

# Import PDF processor
from pdf_processor import SilentPDFProcessor

# FastAPI untuk health check endpoint
from fastapi import FastAPI
import uvicorn


class DocumentWorker:
    def __init__(self):
        """Initialize the document worker with GCP clients"""

        # Environment variables
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "bni-prod-dma-bnimove-ai")
        self.subscription_name = os.getenv("PUBSUB_SUBSCRIPTION", "document-processing-worker")
        self.results_topic = os.getenv("PUBSUB_RESULTS_TOPIC", "document-processing-results")
        self.firestore_database = os.getenv("FIRESTORE_DATABASE", "document-processing-firestore")
        self.max_workers = int(os.getenv("MAX_WORKERS", "4"))
        self.port = int(os.getenv("PORT", "8080"))

        # OpenWebUI configuration for PDF processor
        self.openwebui_config = {
            "api_key": os.getenv("OPENWEBUI_API_KEY", "dummy-api-key"),
            "base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:8080"),
            "model": os.getenv("OPENWEBUI_MODEL", "image-screening-shmshm-elektronik"),
        }

        # Initialize GCP clients
        self.storage_client = storage.Client(project=self.project_id)
        self.subscriber = pubsub_v1.SubscriberClient()
        self.publisher = pubsub_v1.PublisherClient()
        self.firestore_client = firestore.Client(
            project=self.project_id,
            database=self.firestore_database,
        )

        # Paths
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_name
        )
        self.results_topic_path = self.publisher.topic_path(
            self.project_id, self.results_topic
        )

        # Worker status
        self.is_running = False
        self.last_heartbeat = datetime.now(timezone.utc)
        self.processed_jobs = 0
        self.failed_jobs = 0

        print(f"✅ Worker initialized for project: {self.project_id}")
        print(f"Subscription: {self.subscription_name}")
        print(f"Results topic: {self.results_topic}")
        print(f"Max workers: {self.max_workers}")
        print(f"HTTP Port: {self.port}")

    def download_file_from_gcs(self, gcs_path: str, local_path: str) -> bool:
        """Download file from GCS to local filesystem"""
        try:
            if not gcs_path.startswith("gs://"):
                raise ValueError(f"Invalid GCS path: {gcs_path}")

            path_parts = gcs_path[5:].split("/", 1)
            bucket_name = path_parts[0]
            blob_name = path_parts[1]

            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.download_to_filename(local_path)

            print(f"✅ Downloaded: {gcs_path} -> {local_path}")
            return True

        except Exception as e:
            print(f"❌ GCS download failed: {e}")
            return False

    def update_job_status(
        self, job_id: str, status: str, result: Dict = None, error: str = None
    ):
        """Update job status in Firestore"""
        try:
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

            doc_ref.update(update_data)
            print(f"✅ Job {job_id} status updated to: {status}")

        except Exception as e:
            print(f"❌ Firestore update failed: {e}")

    def publish_result(self, job_id: str, result: Dict, status: str):
        """Publish result to results topic"""
        try:
            message_data = {
                "job_id": job_id,
                "status": status,
                "result": result,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

            message_json = json.dumps(message_data).encode("utf-8")
            future = self.publisher.publish(self.results_topic_path, message_json)
            message_id = future.result()

            print(f"✅ Result published: {message_id}")

        except Exception as e:
            print(f"❌ Result publish failed: {e}")

    def process_document(self, job_data: Dict) -> Dict:
        """Process document using PDF processor"""
        job_id = job_data["job_id"]
        document_type = job_data["document_type"]
        gcs_path = job_data["gcs_path"]
        filename = job_data["filename"]
        model_name = job_data.get("model_name")

        print(f"Processing job {job_id}: {document_type} - {filename}")
        start_time = time.time()

        try:
            with tempfile.NamedTemporaryFile(
                suffix=os.path.splitext(filename)[1], delete=False
            ) as tmp_file:
                tmp_path = tmp_file.name

            try:
                if not self.download_file_from_gcs(gcs_path, tmp_path):
                    raise Exception("Failed to download file from GCS")

                config = self.openwebui_config.copy()
                config["document_type"] = document_type
                if model_name:
                    config["model"] = model_name

                processor = SilentPDFProcessor(config)
                processor.enable_logging = True

                print(f"Processing with model: {config['model']}")
                result = processor.process_file(tmp_path)

                processing_time = time.time() - start_time
                print(f"✅ Processing completed in {processing_time:.2f}s")

                return {
                    "success": True,
                    "result": result,
                    "processing_time": round(processing_time, 2),
                    "model_used": config["model"],
                    "document_type": document_type,
                }

            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Processing failed: {str(e)}"
            print(f"❌ {error_msg}")

            return {
                "success": False,
                "error": error_msg,
                "processing_time": round(processing_time, 2),
                "traceback": traceback.format_exc(),
            }

    def process_single_message(self, message_data: Dict, ack_id: str):
        """Process a single message - FIXED untuk prevent duplicate after success"""
        job_id = None

        try:
            # === STEP 1: BASIC VALIDATION ===
            if not isinstance(message_data, dict):
                print(f"❌ Invalid message format: {type(message_data)}")
                self.subscriber.modify_ack_deadline(
                    subscription=self.subscription_path,
                    ack_ids=[ack_id],
                    ack_deadline_seconds=0,
                )
                return

            # === STEP 2: EXTRACT JOB_ID FIRST ===
            job_id = message_data.get("job_id", "unknown")
            print(f"🔍 Processing message for job: {job_id}")

            # === STEP 3: CHECK IF JOB ALREADY COMPLETED (CRITICAL FIX) ===
            if job_id != "unknown":
                try:
                    doc_ref = self.firestore_client.collection("jobs").document(job_id)
                    existing_job = doc_ref.get()

                    if existing_job.exists:
                        job_status = existing_job.to_dict().get("status")
                        print(f"🔍 Existing job status: {job_status}")

                        if job_status in ["completed", "failed"]:
                            print(
                                f"⚠️ Job {job_id} already {job_status} - SKIPPING and ACKing message"
                            )
                            # CRITICAL: ACK the message to prevent retry
                            self.subscriber.acknowledge(
                                subscription=self.subscription_path,
                                ack_ids=[ack_id],
                            )
                            print(f"✅ Duplicate message acknowledged: {ack_id[:10]}...")
                            return
                except Exception as e:
                    print(f"⚠️ Could not check job status: {e} - proceeding anyway")

            # === STEP 4: SIMPLE FIELD CHECK (NO STRICT VALIDATION) ===
            required_fields = ["job_id", "document_type", "gcs_path", "filename"]
            available_fields = list(message_data.keys())
            missing_fields = [field for field in required_fields if field not in message_data]

            if missing_fields:
                print(f"❌ Missing fields: {missing_fields}")
                print(f"❌ Available fields: {available_fields}")
                # NACK malformed messages
                self.subscriber.modify_ack_deadline(
                    subscription=self.subscription_path,
                    ack_ids=[ack_id],
                    ack_deadline_seconds=0,
                )
                print(f"❌ Malformed message nacked: {ack_id[:10]}...")
                return

            # === STEP 5: EXTRACT FIELDS SAFELY ===
            document_type = str(message_data["document_type"]).strip()
            gcs_path = str(message_data["gcs_path"]).strip()
            filename = str(message_data["filename"]).strip()

            print(f"✅ Message validation passed")
            print(f"🔨 Processing job: {job_id}")
            print(f"📄 Document type: {document_type}")
            print(f"📁 File: {filename}")

            # === STEP 6: UPDATE STATUS TO PROCESSING ===
            self.update_job_status(job_id, "processing")

            # === STEP 7: PROCESS DOCUMENT ===
            process_result = self.process_document(message_data)

            # === STEP 8: HANDLE RESULTS AND ACK IMMEDIATELY ===
            if process_result["success"]:
                # Update job status
                self.update_job_status(job_id, "completed", result=process_result["result"])

                # Publish results (in try/catch to not affect ACK)
                try:
                    self.publish_result(job_id, process_result, "completed")
                except Exception as pub_error:
                    print(f"⚠️ Result publish failed: {pub_error} - but job completed")

                print(f"✅ Job {job_id} completed successfully")

            else:
                # Update job status
                self.update_job_status(job_id, "failed", error=process_result["error"])

                # Publish results (in try/catch to not affect ACK)
                try:
                    self.publish_result(job_id, process_result, "failed")
                except Exception as pub_error:
                    print(f"⚠️ Result publish failed: {pub_error} - but job marked failed")

                print(f"❌ Job {job_id} failed: {process_result.get('error', 'Unknown error')}")

            # === STEP 9: CRITICAL - ALWAYS ACK SUCCESSFUL PROCESSING ===
            # Whether job succeeded or failed, if we processed it, ACK the message
            self.subscriber.acknowledge(
                subscription=self.subscription_path,
                ack_ids=[ack_id],
            )
            print(f"✅ Message acknowledged: {ack_id[:10]}...")
            print(f"✅ Processing complete for job: {job_id}")

        except Exception as e:
            error_msg = f"Unexpected processing error: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"❌ Job ID: {job_id}")
            print(f"❌ Traceback: {traceback.format_exc()}")

            # Try to mark job as failed
            if job_id and job_id != "unknown":
                try:
                    self.update_job_status(job_id, "failed", error=error_msg)
                    print(f"✅ Job {job_id} marked as failed")

                    # ACK the message since we handled it (even if failed)
                    self.subscriber.acknowledge(
                        subscription=self.subscription_path,
                        ack_ids=[ack_id],
                    )
                    print(f"✅ Failed job message acknowledged: {ack_id[:10]}...")

                except Exception as update_error:
                    print(f"❌ Could not update failed job: {update_error}")
                    # NACK only if we can't handle it at all
                    self.subscriber.modify_ack_deadline(
                        subscription=self.subscription_path,
                        ack_ids=[ack_id],
                        ack_deadline_seconds=0,
                    )
                    print(f"❌ Unhandleable error - message nacked: {ack_id[:10]}...")
            else:
                # No job_id, NACK the malformed message
                self.subscriber.modify_ack_deadline(
                    subscription=self.subscription_path,
                    ack_ids=[ack_id],
                    ack_deadline_seconds=0,
                )
                print(f"❌ No job_id - message nacked: {ack_id[:10]}...")

    # CRITICAL DEBUGGING: Add method untuk check duplicates
    def check_duplicate_processing(self):
        """Check untuk jobs yang diproses multiple times"""
        try:
            # Query recent jobs
            recent_jobs = (
                self.firestore_client.collection("jobs")
                .order_by("created_at", direction=firestore.Query.DESCENDING)
                .limit(50)
                .get()
            )

            job_counts = {}
            for job_doc in recent_jobs:
                job_data = job_doc.to_dict()
                job_id = job_data.get("job_id")
                if job_id:
                    job_counts[job_id] = job_counts.get(job_id, 0) + 1

            duplicates = {jid: count for jid, count in job_counts.items() if count > 1}
            if duplicates:
                print(f"⚠️ Found duplicate jobs: {duplicates}")
            else:
                print(f"✅ No duplicate jobs found")

        except Exception as e:
            print(f"❌ Duplicate check failed: {e}")

    def start_polling_worker(self):
        """Start the worker using simple polling"""
        print(f"Starting worker with ultra-simple polling...")
        print(f"Polling subscription: {self.subscription_path}")

        self.is_running = True
        consecutive_empty = 0
        max_empty_polls = 6

        try:
            while self.is_running:
                try:
                    # Update heartbeat
                    self.last_heartbeat = datetime.now(timezone.utc)

                    # Pull messages
                    response = self.subscriber.pull(
                        subscription=self.subscription_path,
                        max_messages=self.max_workers,
                    )

                    if response.received_messages:
                        consecutive_empty = 0
                        print(f"Received {len(response.received_messages)} messages")

                        # Process each message
                        for received_message in response.received_messages:
                            try:
                                # Enhanced JSON parsing dengan error handling
                                raw_data = received_message.message.data

                                if not raw_data:
                                    print(f"❌ Empty message data")
                                    # NACK empty messages
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                    continue

                                # Decode dan parse JSON
                                try:
                                    decoded_data = raw_data.decode("utf-8")
                                    message_data = json.loads(decoded_data)
                                except UnicodeDecodeError as e:
                                    print(f"❌ Unicode decode error: {e}")
                                    print(f"❌ Raw data: {raw_data[:100]}...")
                                    # NACK malformed messages
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                    continue
                                except json.JSONDecodeError as e:
                                    print(f"❌ JSON decode error: {e}")
                                    print(f"❌ Decoded data: {decoded_data[:100]}...")
                                    # NACK malformed JSON
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                    continue

                                # Process the job
                                self.process_single_message(
                                    message_data, received_message.ack_id
                                )

                            except Exception as e:
                                print(f"❌ Message handling error: {e}")
                                print(f"❌ Traceback: {traceback.format_exc()}")
                                # NACK on any error
                                try:
                                    self.subscriber.modify_ack_deadline(
                                        subscription=self.subscription_path,
                                        ack_ids=[received_message.ack_id],
                                        ack_deadline_seconds=0,
                                    )
                                except Exception as nack_error:
                                    print(f"❌ Could not NACK message: {nack_error}")
                    else:
                        consecutive_empty += 1
                        if consecutive_empty == 1:
                            print(f"No messages available")
                        elif consecutive_empty >= max_empty_polls:
                            print(f"Worker heartbeat - active and listening...")
                            consecutive_empty = 0

                        # Sleep between polls when no messages
                        time.sleep(5)

                except KeyboardInterrupt:
                    print("\nWorker shutdown requested")
                    break
                except Exception as e:
                    print(f"❌ Polling error: {e}")
                    print(traceback.format_exc())
                    time.sleep(10)  # Wait before retrying

        except Exception as e:
            print(f"❌ Worker failed: {e}")
            print(traceback.format_exc())
            raise
        finally:
            self.is_running = False

        print("✅ Worker shutdown complete")

    def get_health_status(self):
        """Get worker health status"""
        now = datetime.now(timezone.utc)
        time_since_heartbeat = (now - self.last_heartbeat).total_seconds()

        return {
            "status": "healthy"
            if self.is_running and time_since_heartbeat < 60
            else "unhealthy",
            "is_running": self.is_running,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "time_since_heartbeat": round(time_since_heartbeat, 2),
            "processed_jobs": self.processed_jobs,
            "failed_jobs": self.failed_jobs,
            "success_rate": round(
                self.processed_jobs
                / max(1, self.processed_jobs + self.failed_jobs)
                * 100,
                2,
            ),
            "subscription": self.subscription_name,
            "project_id": self.project_id,
        }


# Global worker instance
worker = None

# FastAPI app for health checks
app = FastAPI(title="Document Processing Worker", version="1.0.0")


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Document Processing Worker",
        "status": "running",
        "version": "1.0.0",
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    global worker
    if worker:
        return worker.get_health_status()
    else:
        return {"status": "initializing", "message": "Worker not initialized yet"}


@app.get("/metrics")
async def metrics():
    """Metrics endpoint"""
    global worker
    if worker:
        status = worker.get_health_status()
        return {
            "processed_jobs": status["processed_jobs"],
            "failed_jobs": status["failed_jobs"],
            "success_rate": status["success_rate"],
            "uptime_seconds": status["time_since_heartbeat"],
        }
    else:
        return {"message": "Worker not initialized"}


def start_worker_in_thread():
    """Start worker in background thread"""
    global worker
    try:
        worker = DocumentWorker()
        worker.start_polling_worker()
    except Exception as e:
        print(f"❌ Worker thread failed: {e}")


def main():
    """Main entry point"""
    global worker

    try:
        print("Initializing Document Processing Worker...")

        # Initialize worker
        worker = DocumentWorker()

        # Start worker in background thread
        worker_thread = threading.Thread(
            target=worker.start_polling_worker, daemon=True
        )
        worker_thread.start()

        print(f"✅ Worker thread started")
        print(f"🚀 Starting HTTP server on port {worker.port}")

        # Start HTTP server for health checks
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=worker.port,
            log_level="info",
        )

    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
        if worker:
            worker.is_running = False
    except Exception as e:
        print(f"❌ Worker failed to start: {e}")
        print(traceback.format_exc())
        exit(1)


if __name__ == "__main__":
    main()

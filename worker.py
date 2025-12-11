import os
import json
import time
import tempfile
import traceback
from datetime import datetime, timezone
from typing import Dict, Any

# Google Cloud imports
from google.cloud import storage, pubsub_v1, firestore
from google.api_core import exceptions as gcp_exceptions

# Import PDF processor
from pdf_processor import SilentPDFProcessor

class DocumentWorker:
    def __init__(self):
        """Initialize the document worker with GCP clients"""
        
        # Environment variables
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "bni-prod-dma-bnimove-ai")
        self.subscription_name = os.getenv("PUBSUB_SUBSCRIPTION", "document-processing-request-sub")
        self.results_topic = os.getenv("PUBSUB_RESULTS_TOPIC", "document-processing-results")
        self.firestore_database = os.getenv("FIRESTORE_DATABASE", "document-processing-firestore")
        self.max_workers = int(os.getenv("MAX_WORKERS", "4"))
        
        # OpenWebUI configuration for PDF processor
        self.openwebui_config = {
            "api_key": os.getenv("OPENWEBUI_API_KEY", "dummy-api-key"),
            "base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:8080"),
            "model": os.getenv("OPENWEBUI_MODEL", "image-screening-shmshm-elektronik")
        }
        
        # Initialize GCP clients
        self.storage_client = storage.Client(project=self.project_id)
        self.subscriber = pubsub_v1.SubscriberClient()
        self.publisher = pubsub_v1.PublisherClient()
        self.firestore_client = firestore.Client(
            project=self.project_id,
            database=self.firestore_database
        )
        
        # Paths
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_name
        )
        self.results_topic_path = self.publisher.topic_path(
            self.project_id, self.results_topic
        )
        
        print(f"✅ Worker initialized for project: {self.project_id}")
        print(f"Subscription: {self.subscription_name}")
        print(f"Results topic: {self.results_topic}")
        print(f"Max workers: {self.max_workers}")

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

    def update_job_status(self, job_id: str, status: str, result: Dict = None, error: str = None):
        """Update job status in Firestore"""
        try:
            doc_ref = self.firestore_client.collection("jobs").document(job_id)
            
            update_data = {
                "status": status,
                "updated_at": datetime.now(timezone.utc)
            }
            
            if status == "completed":
                update_data["completed_at"] = datetime.now(timezone.utc)
                if result:
                    update_data["result"] = result
            elif status == "failed":
                update_data["completed_at"] = datetime.now(timezone.utc)
                if error:
                    update_data["error"] = error
            
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
                "processed_at": datetime.now(timezone.utc).isoformat()
            }
            
            message_json = json.dumps(message_data).encode('utf-8')
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
            with tempfile.NamedTemporaryFile(suffix=os.path.splitext(filename)[1], delete=False) as tmp_file:
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
                    "document_type": document_type
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
                "traceback": traceback.format_exc()
            }

    def process_single_message(self, message_data: Dict, ack_id: str):
        """Process a single message"""
        job_id = message_data["job_id"]
        print(f"📨 Processing job: {job_id}")
        
        try:
            # Update status to processing
            self.update_job_status(job_id, "processing")
            
            # Process the document
            process_result = self.process_document(message_data)
            
            if process_result["success"]:
                self.update_job_status(job_id, "completed", result=process_result["result"])
                self.publish_result(job_id, process_result, "completed")
                print(f"✅ Job {job_id} completed successfully")
            else:
                self.update_job_status(job_id, "failed", error=process_result["error"])
                self.publish_result(job_id, process_result, "failed")
                print(f"❌ Job {job_id} failed")
            
            # Acknowledge the message
            self.subscriber.acknowledge(
                subscription=self.subscription_path,
                ack_ids=[ack_id]
            )
            print(f"✅ Message acknowledged: {ack_id[:10]}...")
            
        except Exception as e:
            print(f"❌ Message processing error: {e}")
            # Nack the message by modifying ack deadline to 0
            self.subscriber.modify_ack_deadline(
                subscription=self.subscription_path,
                ack_ids=[ack_id],
                ack_deadline_seconds=0
            )
            print(f"❌ Message nacked: {ack_id[:10]}...")

    def start_worker(self):
        """Start the worker using simple polling"""
        print(f"Starting worker with ultra-simple polling...")
        print(f"Polling subscription: {self.subscription_path}")
        
        consecutive_empty = 0
        max_empty_polls = 6  # Print heartbeat every 30 seconds (6 * 5s)
        
        try:
            while True:
                try:
                    # ULTRA SIMPLE: Minimal pull request
                    response = self.subscriber.pull(
                        subscription=self.subscription_path,
                        max_messages=self.max_workers
                    )
                    
                    if response.received_messages:
                        consecutive_empty = 0
                        print(f"Received {len(response.received_messages)} messages")
                        
                        # Process each message
                        for received_message in response.received_messages:
                            try:
                                # Parse message
                                message_data = json.loads(received_message.message.data.decode('utf-8'))
                                
                                # Process the job
                                self.process_single_message(message_data, received_message.ack_id)
                                
                            except Exception as e:
                                print(f"❌ Message parsing error: {e}")
                                # Nack on parsing errors
                                self.subscriber.modify_ack_deadline(
                                    subscription=self.subscription_path,
                                    ack_ids=[received_message.ack_id],
                                    ack_deadline_seconds=0
                                )
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
        
        print("✅ Worker shutdown complete")

def main():
    """Main entry point"""
    try:
        print("Initializing Document Processing Worker...")
        worker = DocumentWorker()
        worker.start_worker()
    except KeyboardInterrupt:
        print("\n🛑 Worker stopped by user")
    except Exception as e:
        print(f"❌ Worker failed to start: {e}")
        print(traceback.format_exc())
        exit(1)

if __name__ == "__main__":
    main()

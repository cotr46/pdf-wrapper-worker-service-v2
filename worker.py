# """
# Document Processing Worker Service
# Subscribes to Pub/Sub, processes documents, and updates Firestore
# """

# import os
# import json
# import time
# import tempfile
# import traceback
# from datetime import datetime, timezone
# from concurrent.futures import ThreadPoolExecutor
# from typing import Dict, Any

# # Google Cloud imports
# from google.cloud import storage, pubsub_v1, firestore
# from google.cloud.pubsub_v1.subscriber import message
# from google.api_core import exceptions as gcp_exceptions

# # Import PDF processor
# from pdf_processor import SilentPDFProcessor

# class DocumentWorker:
#     def __init__(self):
#         """Initialize the document worker with GCP clients"""
        
#         # Environment variables
#         self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "bni-prod-dma-bnimove-ai")
#         self.subscription_name = os.getenv("PUBSUB_SUBSCRIPTION", "document-processing-request-sub")
#         self.results_topic = os.getenv("PUBSUB_RESULTS_TOPIC", "document-processing-results")
#         self.firestore_database = os.getenv("FIRESTORE_DATABASE", "document-processing-firestore")
#         self.max_workers = int(os.getenv("MAX_WORKERS", "4"))
        
#         # OpenWebUI configuration for PDF processor
#         self.openwebui_config = {
#             "api_key": os.getenv("OPENWEBUI_API_KEY", "dummy-api-key"),
#             "base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:8080"),
#             "model": os.getenv("OPENWEBUI_MODEL", "image-screening-shmshm-elektronik")
#         }
        
#         # Initialize GCP clients
#         self.storage_client = storage.Client(project=self.project_id)
#         self.subscriber = pubsub_v1.SubscriberClient()
#         self.publisher = pubsub_v1.PublisherClient()
#         self.firestore_client = firestore.Client(
#             project=self.project_id,
#             database=self.firestore_database
#         )
        
#         # Paths
#         self.subscription_path = self.subscriber.subscription_path(
#             self.project_id, self.subscription_name
#         )
#         self.results_topic_path = self.publisher.topic_path(
#             self.project_id, self.results_topic
#         )
        
#         print(f"✅ Worker initialized for project: {self.project_id}")
#         print(f"📥 Subscription: {self.subscription_name}")
#         print(f"📤 Results topic: {self.results_topic}")
#         print(f"👥 Max workers: {self.max_workers}")

#     def download_file_from_gcs(self, gcs_path: str, local_path: str) -> bool:
#         """Download file from GCS to local filesystem"""
#         try:
#             # Parse GCS path: gs://bucket/path/to/file
#             if not gcs_path.startswith("gs://"):
#                 raise ValueError(f"Invalid GCS path: {gcs_path}")
            
#             # Extract bucket and blob name
#             path_parts = gcs_path[5:].split("/", 1)  # Remove 'gs://' and split
#             bucket_name = path_parts[0]
#             blob_name = path_parts[1]
            
#             # Download file
#             bucket = self.storage_client.bucket(bucket_name)
#             blob = bucket.blob(blob_name)
            
#             blob.download_to_filename(local_path)
            
#             print(f"✅ Downloaded: {gcs_path} -> {local_path}")
#             return True
            
#         except Exception as e:
#             print(f"❌ GCS download failed: {e}")
#             return False

#     def update_job_status(self, job_id: str, status: str, result: Dict = None, error: str = None):
#         """Update job status in Firestore"""
#         try:
#             doc_ref = self.firestore_client.collection("jobs").document(job_id)
            
#             update_data = {
#                 "status": status,
#                 "updated_at": datetime.now(timezone.utc)
#             }
            
#             if status == "completed":
#                 update_data["completed_at"] = datetime.now(timezone.utc)
#                 if result:
#                     update_data["result"] = result
#             elif status == "failed":
#                 update_data["completed_at"] = datetime.now(timezone.utc)
#                 if error:
#                     update_data["error"] = error
            
#             doc_ref.update(update_data)
            
#             print(f"✅ Job {job_id} status updated to: {status}")
            
#         except Exception as e:
#             print(f"❌ Firestore update failed: {e}")

#     def publish_result(self, job_id: str, result: Dict, status: str):
#         """Publish result to results topic"""
#         try:
#             message_data = {
#                 "job_id": job_id,
#                 "status": status,
#                 "result": result,
#                 "processed_at": datetime.now(timezone.utc).isoformat()
#             }
            
#             message_json = json.dumps(message_data).encode('utf-8')
#             future = self.publisher.publish(self.results_topic_path, message_json)
#             message_id = future.result()
            
#             print(f"✅ Result published: {message_id}")
            
#         except Exception as e:
#             print(f"❌ Result publish failed: {e}")

#     def process_document(self, job_data: Dict) -> Dict:
#         """Process document using PDF processor"""
#         job_id = job_data["job_id"]
#         document_type = job_data["document_type"] 
#         gcs_path = job_data["gcs_path"]
#         filename = job_data["filename"]
#         model_name = job_data.get("model_name")
        
#         print(f"🔄 Processing job {job_id}: {document_type} - {filename}")
        
#         start_time = time.time()
        
#         try:
#             # Create temporary file
#             with tempfile.NamedTemporaryFile(suffix=os.path.splitext(filename)[1], delete=False) as tmp_file:
#                 tmp_path = tmp_file.name
            
#             try:
#                 # Download file from GCS
#                 if not self.download_file_from_gcs(gcs_path, tmp_path):
#                     raise Exception("Failed to download file from GCS")
                
#                 # Configure processor for this specific document type and model
#                 config = self.openwebui_config.copy()
#                 config["document_type"] = document_type
#                 if model_name:
#                     config["model"] = model_name
                
#                 processor = SilentPDFProcessor(config)
#                 processor.enable_logging = True  # Enable logging for worker
                
#                 # Process the document
#                 print(f"📄 Processing with model: {config['model']}")
#                 result = processor.process_file(tmp_path)
                
#                 processing_time = time.time() - start_time
#                 print(f"✅ Processing completed in {processing_time:.2f}s")
                
#                 return {
#                     "success": True,
#                     "result": result,
#                     "processing_time": round(processing_time, 2),
#                     "model_used": config["model"],
#                     "document_type": document_type
#                 }
                
#             finally:
#                 # Clean up temporary file
#                 if os.path.exists(tmp_path):
#                     os.unlink(tmp_path)
                
#         except Exception as e:
#             processing_time = time.time() - start_time
#             error_msg = f"Processing failed: {str(e)}"
#             print(f"❌ {error_msg}")
            
#             return {
#                 "success": False,
#                 "error": error_msg,
#                 "processing_time": round(processing_time, 2),
#                 "traceback": traceback.format_exc()
#             }

#     def handle_message(self, message: message.Message):
#         """Handle incoming Pub/Sub message"""
#         try:
#             # Parse message data
#             message_data = json.loads(message.data.decode('utf-8'))
#             job_id = message_data["job_id"]
            
#             print(f"📨 Received message for job: {job_id}")
            
#             # Update status to processing
#             self.update_job_status(job_id, "processing")
            
#             # Process the document
#             process_result = self.process_document(message_data)
            
#             if process_result["success"]:
#                 # Update job as completed
#                 self.update_job_status(
#                     job_id, 
#                     "completed", 
#                     result=process_result["result"]
#                 )
                
#                 # Publish success result
#                 self.publish_result(job_id, process_result, "completed")
                
#                 print(f"✅ Job {job_id} completed successfully")
                
#             else:
#                 # Update job as failed
#                 self.update_job_status(
#                     job_id,
#                     "failed", 
#                     error=process_result["error"]
#                 )
                
#                 # Publish failure result
#                 self.publish_result(job_id, process_result, "failed")
                
#                 print(f"❌ Job {job_id} failed")
            
#             # Acknowledge message
#             message.ack()
            
#         except Exception as e:
#             print(f"❌ Message handling failed: {e}")
#             print(traceback.format_exc())
            
#             # Try to update job status if we have job_id
#             try:
#                 if 'message_data' in locals() and 'job_id' in message_data:
#                     self.update_job_status(
#                         message_data["job_id"],
#                         "failed",
#                         error=f"Worker error: {str(e)}"
#                     )
#             except:
#                 pass
            
#             # Nack message to retry later
#             message.nack()

#     def start_worker(self):
#         """Start the worker to listen for messages"""
#         print(f"🚀 Starting worker...")
#         print(f"📡 Listening on: {self.subscription_path}")
        
#         # Flow control settings
#         flow_control = pubsub_v1.types.FlowControl(max_messages=self.max_workers)
        
#         # Start listening for messages
#         with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
#             try:
#                 streaming_pull_future = self.subscriber.pull(
#                     request={
#                         "subscription": self.subscription_path,
#                         "max_messages": self.max_workers,
#                     },
#                     flow_control=flow_control,
#                 )
                
#                 print(f"✅ Worker started. Waiting for messages...")
                
#                 # Keep the worker running
#                 try:
#                     streaming_pull_future.result()
#                 except KeyboardInterrupt:
#                     print("\n🛑 Received interrupt signal")
#                     streaming_pull_future.cancel()
                    
#             except Exception as e:
#                 print(f"❌ Worker error: {e}")
#                 raise

# def main():
#     """Main entry point"""
#     try:
#         worker = DocumentWorker()
#         worker.start_worker()
#     except KeyboardInterrupt:
#         print("\n🛑 Worker stopped by user")
#     except Exception as e:
#         print(f"❌ Worker failed to start: {e}")
#         print(traceback.format_exc())

# if __name__ == "__main__":
#     main()

"""
Document Processing Worker Service - FINAL CORRECTED VERSION
Subscribes to Pub/Sub, processes documents, and updates Firestore
"""

import os
import json
import time
import tempfile
import traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
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
        print(f"📥 Subscription: {self.subscription_name}")
        print(f"📤 Results topic: {self.results_topic}")
        print(f"👥 Max workers: {self.max_workers}")

    def download_file_from_gcs(self, gcs_path: str, local_path: str) -> bool:
        """Download file from GCS to local filesystem"""
        try:
            # Parse GCS path: gs://bucket/path/to/file
            if not gcs_path.startswith("gs://"):
                raise ValueError(f"Invalid GCS path: {gcs_path}")
            
            # Extract bucket and blob name
            path_parts = gcs_path[5:].split("/", 1)  # Remove 'gs://' and split
            bucket_name = path_parts[0]
            blob_name = path_parts[1]
            
            # Download file
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
        
        print(f"🔄 Processing job {job_id}: {document_type} - {filename}")
        
        start_time = time.time()
        
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(suffix=os.path.splitext(filename)[1], delete=False) as tmp_file:
                tmp_path = tmp_file.name
            
            try:
                # Download file from GCS
                if not self.download_file_from_gcs(gcs_path, tmp_path):
                    raise Exception("Failed to download file from GCS")
                
                # Configure processor for this specific document type and model
                config = self.openwebui_config.copy()
                config["document_type"] = document_type
                if model_name:
                    config["model"] = model_name
                
                processor = SilentPDFProcessor(config)
                processor.enable_logging = True  # Enable logging for worker
                
                # Process the document
                print(f"📄 Processing with model: {config['model']}")
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
                # Clean up temporary file
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

    def handle_message(self, message):
        """Handle incoming Pub/Sub message"""
        try:
            # Parse message data
            message_data = json.loads(message.data.decode('utf-8'))
            job_id = message_data["job_id"]
            
            print(f"📨 Received message for job: {job_id}")
            
            # Update status to processing
            self.update_job_status(job_id, "processing")
            
            # Process the document
            process_result = self.process_document(message_data)
            
            if process_result["success"]:
                # Update job as completed
                self.update_job_status(
                    job_id, 
                    "completed", 
                    result=process_result["result"]
                )
                
                # Publish success result
                self.publish_result(job_id, process_result, "completed")
                
                print(f"✅ Job {job_id} completed successfully")
                
            else:
                # Update job as failed
                self.update_job_status(
                    job_id,
                    "failed", 
                    error=process_result["error"]
                )
                
                # Publish failure result
                self.publish_result(job_id, process_result, "failed")
                
                print(f"❌ Job {job_id} failed")
            
            # Acknowledge message
            message.ack()
            
        except Exception as e:
            print(f"❌ Message handling failed: {e}")
            print(traceback.format_exc())
            
            # Try to update job status if we have job_id
            try:
                if 'message_data' in locals() and 'job_id' in message_data:
                    self.update_job_status(
                        message_data["job_id"],
                        "failed",
                        error=f"Worker error: {str(e)}"
                    )
            except:
                pass
            
            # Nack message to retry later
            message.nack()

    def start_worker(self):
        """Start the worker to listen for messages"""
        print(f"🚀 Starting worker...")
        print(f"📡 Listening on: {self.subscription_path}")
        
        # Flow control settings
        flow_control = pubsub_v1.types.FlowControl(max_messages=self.max_workers)
        
        # Callback function for processing messages
        def callback_wrapper(message):
            """Wrapper function to handle messages in thread pool"""
            try:
                self.handle_message(message)
            except Exception as e:
                print(f"❌ Callback error: {e}")
                message.nack()
        
        try:
            # Start streaming pull with callback
            streaming_pull_future = self.subscriber.pull(
                subscription=self.subscription_path,
                callback=callback_wrapper,
                flow_control=flow_control
            )
            
            print(f"✅ Worker started successfully!")
            print(f"📡 Listening for messages on: {self.subscription_path}")
            
            # Keep the main thread alive
            try:
                while True:
                    time.sleep(60)
                    print(f"🔄 Worker heartbeat - active and listening...")
                    
            except KeyboardInterrupt:
                print("\n🛑 Shutting down worker...")
                streaming_pull_future.cancel()
                streaming_pull_future.result()  # Block until shutdown complete
                print("✅ Worker shutdown complete")
                
        except Exception as e:
            print(f"❌ Worker failed to start: {e}")
            print(traceback.format_exc())
            raise

def main():
    """Main entry point"""
    try:
        print("🚀 Initializing Document Processing Worker...")
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

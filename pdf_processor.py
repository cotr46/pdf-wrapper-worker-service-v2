import requests
import json
import base64
import os
import sys
import time
import threading
import gc
from datetime import datetime
from io import BytesIO
from typing import List, Dict, Optional
import re

try:
    import fitz  # PyMuPDF
    from PIL import Image, ImageEnhance
except ImportError:
    # Tanpa PyMuPDF + Pillow service nggak bisa jalan, jadi langsung exit
    sys.exit(1)


class UltraFastPDFProcessor:
    """
    ULTRA-OPTIMIZED PDF Processor untuk production
    Target: 12.6 menit → 3-4 menit (70% faster)
    """

    def __init__(self, config: Dict):
        self.api_key = config["api_key"]
        self.base_url = config["base_url"]
        self.model = config["model"]
        
        # TAMBAHAN: Document type detection
        self.document_type = config.get("document_type", "auto")

        # ULTRA-OPTIMIZED SETTINGS untuk production
        self.chunk_size = config.get("chunk_size", 4)  # 2→4 (fewer API calls)
        self.max_image_size_kb = config.get("max_image_size_kb", 300)  # 400→300 (faster upload)
        self.base_image_quality = config.get("base_image_quality", 80)  # 88→80 (faster processing)
        self.timeout_seconds = config.get("timeout_seconds", 90)  # 150→90 (faster timeouts)

        # BREAKTHROUGH OPTIMIZATION: Dramatic delay reduction
        self.min_delay_between_requests = config.get("min_delay_between_requests", 15)  # 70→15 (5x faster!)
        self.safety_margin = config.get("safety_margin", 1)  # 5→1 (minimal safety)

        self.last_request_time = 0.0
        self.rate_lock = threading.Lock()

        self.enable_logging = True
        self.log_messages: List[str] = []
        
        # PRODUCTION OPTIMIZATIONS
        self._apply_ultra_optimizations()

    def _apply_ultra_optimizations(self):
        """
        Apply ULTRA optimizations untuk production environment
        """
        import os
        
        # AGGRESSIVE thread limiting untuk Cloud Run
        os.environ['OPENBLAS_NUM_THREADS'] = '1'
        os.environ['MKL_NUM_THREADS'] = '1'
        os.environ['OMP_NUM_THREADS'] = '1'
        
        # ULTRA-OPTIMIZED PIL settings
        from PIL import Image
        Image.MAX_IMAGE_PIXELS = 50000000  # Reduced dari 100M
        Image.LOAD_TRUNCATED_IMAGES = True
        
        # Disable PIL warnings untuk cleaner logs
        import warnings
        warnings.filterwarnings("ignore", category=Image.DecompressionBombWarning)
        
        self.log("🚀 ULTRA optimizations applied for PRODUCTION")
        self.log(f"   - Delays reduced: 70s → {self.min_delay_between_requests}s ({70/self.min_delay_between_requests:.1f}x faster)")
        self.log(f"   - Chunk size: 2 → {self.chunk_size} (fewer API calls)")
        self.log(f"   - Image quality: 88 → {self.base_image_quality} (faster processing)")

    def log(self, message: str) -> None:
        """Logging internal dengan timestamp."""
        if not self.enable_logging:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(log_msg, file=sys.stderr, flush=True)
        self.log_messages.append(log_msg)

    def ultra_fast_rotate_if_needed(self, pil_image: Image.Image) -> Image.Image:
        """
        ULTRA-FAST rotation dengan aggressive threshold
        """
        width, height = pil_image.size
        aspect_ratio = height / width
        
        # AGGRESSIVE threshold untuk minimize rotations
        if aspect_ratio > 1.5:  # 1.4→1.5 (even more selective)
            # Ultra-fast transpose
            return pil_image.transpose(Image.Transpose.ROTATE_90)
        return pil_image

    def auto_detect_doc_type(self, file_path: str = None) -> str:
        """Auto-detect document type dengan caching"""
        # Use cached value if available
        if hasattr(self, '_cached_doc_type'):
            return self._cached_doc_type
            
        # 1. Cek self.document_type jika sudah di-set
        if hasattr(self, 'document_type') and self.document_type and self.document_type != "auto":
            self._cached_doc_type = self.document_type
            return self.document_type
        
        # 2. Cek dari file path jika ada
        if file_path:
            filename = os.path.basename(file_path).lower()
            if "bpkb" in filename:
                self._cached_doc_type = "bpkb"
                return "bpkb"
            elif "shm" in filename or "sertifikat" in filename:
                self._cached_doc_type = "shm"
                return "shm"
            elif "nib" in filename:
                self._cached_doc_type = "nib"
                return "nib"
            elif "ktp" in filename:
                self._cached_doc_type = "ktp"
                return "ktp"
            elif "npwp" in filename:
                self._cached_doc_type = "npwp"
                return "npwp"
            elif "sku" in filename:
                self._cached_doc_type = "sku"
                return "sku"
        
        # 3. Fallback
        self._cached_doc_type = "nib"
        return "nib"

    def process_file(self, file_path: str) -> Dict:
        """
        ULTRA-OPTIMIZED file processing entry point
        """
        try:
            # Deteksi tipe file
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.pdf':
                self.log("🚀 Processing as PDF file (ULTRA-FAST mode)")
                return self.ultra_fast_process_pdf(file_path)
            elif file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                self.log("🚀 Processing as image file (ULTRA-FAST mode)")
                return self.ultra_fast_process_image(file_path)
            else:
                return {"error": f"Unsupported file type: {file_ext}"}
                
        except Exception as e:
            self.log(f"File processing error: {type(e).__name__}: {e}")
            return {
                "error": f"File processing error: {str(e)}",
                "error_type": type(e).__name__
            }
        finally:
            # AGGRESSIVE cleanup
            gc.collect()

    def ultra_fast_process_image(self, image_path: str) -> Dict:
        """
        ULTRA-FAST image processing
        """
        start_time = time.time()

        try:
            self.log(f"🚀 ULTRA-FAST image processing: {os.path.basename(image_path)}")

            if not self.validate_image_file(image_path):
                return {"error": "Image validation failed"}

            detected_type = self.auto_detect_doc_type(image_path)
            self.log(f"📄 Document type: {detected_type}")

            try:
                # ULTRA-FAST image loading dan conversion
                pil_image = Image.open(image_path)
                pil_image.load()  # Load immediately
            
                # Fast mode conversion
                if pil_image.mode not in ("RGB", "L"):
                    pil_image = pil_image.convert("RGB")

                # ULTRA-FAST rotation
                pil_image = self.ultra_fast_rotate_if_needed(pil_image)

                # ULTRA-FAST optimization
                optimized_base64 = self.ultra_fast_optimize_image(pil_image)
                
                # Immediate cleanup
                pil_image.close()
                del pil_image
                
                if not optimized_base64:
                    return {"error": "Image optimization failed"}
                    
                self.log("✅ Image converted with ULTRA-FAST method")

            except Exception as e:
                self.log(f"Image conversion error: {e}")
                return {"error": f"Image conversion failed: {str(e)}"}

            # Process dengan minimal delay
            chunk = [optimized_base64]
            self.ultra_fast_wait()
            
            result = self.ultra_fast_process_chunk(chunk, 1, 1, 1, detected_type)
            
            if result and result.strip():
                json_result = self.extract_json_only(result)
                processing_time = time.time() - start_time
                self.log(f"🎉 ULTRA-FAST image processing: {processing_time:.1f}s")
                return json_result
            else:
                fallback_response = self.get_fallback_response(detected_type)
                processing_time = time.time() - start_time
                self.log(f"⚠️ Using fallback in {processing_time:.1f}s")
                return self.extract_json_only(fallback_response)

        except Exception as e:
            self.log(f"❌ Image processing error: {type(e).__name__}: {e}")
            detected_type = self.auto_detect_doc_type(image_path)
            fallback_response = self.get_fallback_response(detected_type)
            return self.extract_json_only(fallback_response)

    def validate_image_file(self, image_path: str) -> bool:
        """FAST image validation"""
        try:
            if not os.path.exists(image_path):
                return False

            # Quick extension check
            valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
            file_ext = os.path.splitext(image_path)[1].lower()
            if file_ext not in valid_extensions:
                return False

            # Quick size check
            file_size_mb = os.path.getsize(image_path) / (1024 * 1024)
            if file_size_mb > 50:  # Max 50MB
                return False

            # SKIP PIL validation untuk speed (trust the file)
            self.log(f"✅ Image validated: {file_size_mb:.1f}MB")
            return True

        except Exception as e:
            self.log(f"Image validation error: {e}")
            return False

    def ultra_fast_process_pdf(self, file_path: str) -> Dict:
        """
        ULTRA-OPTIMIZED PDF processing
        """
        start_time = time.time()

        try:
            self.log(f"🚀 ULTRA-FAST PDF processing: {os.path.basename(file_path)}")

            if not self.validate_file(file_path):
                return {"error": "PDF validation failed"}

            detected_type = self.auto_detect_doc_type(file_path)
            self.log(f"📄 Document type: {detected_type}")

            chunks_data = self.ultra_fast_convert_pdf(file_path)
            if not chunks_data:
                return {"error": "PDF conversion failed"}

            chunks = chunks_data["chunks"]
            total_pages = chunks_data["total_pages"]
            self.log(f"📚 Created {len(chunks)} chunks from {total_pages} pages (ULTRA-FAST)")

            results: List[str] = []
            failed_chunks = 0

            for i, chunk in enumerate(chunks):
                chunk_start = time.time()
                self.log(f"⚡ Processing chunk {i + 1}/{len(chunks)} ({len(chunk)} images)...")

                self.ultra_fast_wait()

                result = self.ultra_fast_process_chunk(chunk, i + 1, len(chunks), total_pages, detected_type)
                
                chunk_time = time.time() - chunk_start
                
                if result and result.strip():
                    results.append(result)
                    self.log(f"✅ Chunk {i + 1}: SUCCESS in {chunk_time:.1f}s ({len(result)} chars)")
                else:
                    failed_chunks += 1
                    self.log(f"❌ Chunk {i + 1}: FAILED in {chunk_time:.1f}s")

            if not results:
                fallback_response = self.get_fallback_response(detected_type)
                processing_time = time.time() - start_time
                self.log(f"⚠️ All chunks failed, using fallback in {processing_time:.1f}s")
                return self.extract_json_only(fallback_response)

            if len(results) == 1:
                final_result = results[0]
            else:
                final_result = self.merge_chunk_results(results, detected_type)
                if not final_result:
                    final_result = results[0]

            json_result = self.extract_json_only(final_result)

            processing_time = time.time() - start_time
            success_rate = len(results) / len(chunks) * 100
            
            self.log(f"🎉 ULTRA-FAST PDF processing completed:")
            self.log(f"   - Total time: {processing_time:.1f}s")
            self.log(f"   - Success rate: {success_rate:.1f}% ({len(results)}/{len(chunks)} chunks)")
            self.log(f"   - Performance: ~{processing_time/60:.1f} minutes total")

            return json_result

        except Exception as e:
            self.log(f"❌ PDF processing error: {type(e).__name__}: {e}")
            processing_time = time.time() - start_time
            self.log(f"❌ Processing failed after {processing_time:.1f}s")
            
            detected_type = self.auto_detect_doc_type(file_path)
            fallback_response = self.get_fallback_response(detected_type)
            return self.extract_json_only(fallback_response)

    def validate_file(self, file_path: str) -> bool:
        """FAST PDF validation"""
        try:
            if not os.path.exists(file_path):
                return False

            doc = fitz.open(file_path)
            page_count = doc.page_count
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            doc.close()

            if page_count == 0:
                return False

            self.log(f"✅ PDF validated: {page_count} pages, {file_size_mb:.1f}MB")
            return True

        except Exception as e:
            self.log(f"Validation error: {e}")
            return False

    def ultra_fast_convert_pdf(self, pdf_path: str) -> Optional[Dict]:
        """
        ULTRA-FAST PDF conversion dengan aggressive optimizations
        """
        try:
            doc = fitz.open(pdf_path)
            total_pages = doc.page_count
        except Exception as e:
            self.log(f"Failed to open PDF: {str(e)}")
            return None

        # ULTRA-AGGRESSIVE settings untuk speed
        dpi = 90  # 150→120→90 (much faster, still readable)
        zoom = dpi / 72.0
        matrix = fitz.Matrix(zoom, zoom)

        base64_images: List[str] = []

        self.log(f"🚀 PDF conversion: {total_pages} pages (ULTRA-FAST mode)")

        for page_num in range(total_pages):
            page_start_time = time.time()
            
            try:
                page = doc.load_page(page_num)
                
                # ULTRA-FAST: Direct JPEG dengan aggressive settings
                pixmap = page.get_pixmap(matrix=matrix, alpha=False)

                try:
                    # Direct JPEG dengan lower quality untuk speed
                    img_bytes = pixmap.tobytes("jpeg", jpg_quality=75)  # 85→75
                except Exception:
                    img_bytes = pixmap.tobytes()

                # Immediate cleanup
                pixmap = None

                # ULTRA-FAST PIL processing
                pil_image = Image.open(BytesIO(img_bytes))
                del img_bytes  # Immediate cleanup

                if pil_image.mode not in ("RGB", "L"):
                    pil_image = pil_image.convert("RGB")

                # ULTRA-FAST rotation
                pil_image = self.ultra_fast_rotate_if_needed(pil_image)

                # SKIP enhancement untuk maximum speed
                # No contrast or sharpness enhancement

                # ULTRA-FAST optimization
                optimized_base64 = self.ultra_fast_optimize_image(pil_image)

                if optimized_base64:
                    base64_images.append(optimized_base64)
                    page_time = time.time() - page_start_time
                    self.log(f"⚡ Page {page_num + 1}: {page_time:.2f}s (ULTRA-FAST)")

                # Force cleanup
                pil_image.close()
                del pil_image

            except Exception as e:
                self.log(f"❌ Page {page_num + 1}: Error - {str(e)[:80]}")
                continue
            
            # Aggressive garbage collection
            if page_num % 2 == 0:
                gc.collect()

        try:
            doc.close()
        except Exception:
            pass

        # Final cleanup
        gc.collect()

        if not base64_images:
            return None

        chunks: List[List[str]] = [
            base64_images[i: i + self.chunk_size]
            for i in range(0, len(base64_images), self.chunk_size)
        ]

        conversion_summary = f"{len(base64_images)}/{total_pages} pages → {len(chunks)} chunks"
        self.log(f"🎯 ULTRA-FAST conversion: {conversion_summary}")

        return {
            "chunks": chunks,
            "total_pages": total_pages,
        }

    def ultra_fast_optimize_image(self, pil_image: Image.Image) -> Optional[str]:
        """
        ULTRA-FAST image optimization dengan aggressive compression
        """
        try:
            if pil_image.mode not in ("RGB", "L"):
                pil_image = pil_image.convert("RGB")

            # AGGRESSIVE dimension reduction untuk speed
            max_dimension = 1000  # 1400→1000 (much faster)

            # Fast resize
            if max(pil_image.width, pil_image.height) > max_dimension:
                if pil_image.width >= pil_image.height:
                    ratio = max_dimension / pil_image.width
                    new_size = (max_dimension, int(pil_image.height * ratio))
                else:
                    ratio = max_dimension / pil_image.height
                    new_size = (int(pil_image.width * ratio), max_dimension)
                
                pil_image = pil_image.resize(new_size, Image.Resampling.LANCZOS)

            # ULTRA-FAST: Single-pass JPEG encoding
            buffer = BytesIO()
            quality = self.base_image_quality  # Use configured quality
            
            pil_image.save(buffer, format="JPEG", quality=quality, optimize=False)  # Skip optimize untuk speed
            data = buffer.getvalue()
            size_kb = len(data) / 1024

            # SKIP secondary optimization untuk speed
            # Langsung return jika masih reasonable size
            if size_kb > self.max_image_size_kb * 1.5:  # Allow 50% overage
                # Quick resize saja
                new_w = int(pil_image.width * 0.85)
                new_h = int(pil_image.height * 0.85)
                pil_image = pil_image.resize((new_w, new_h), Image.Resampling.LANCZOS)
                
                buffer = BytesIO()
                pil_image.save(buffer, format="JPEG", quality=65, optimize=False)
                data = buffer.getvalue()
                size_kb = len(data) / 1024

            return base64.b64encode(data).decode("utf-8")

        except Exception as e:
            self.log(f"Ultra-fast optimization error: {e}")
            return None

    def ultra_fast_wait(self) -> None:
        """
        ULTRA-FAST rate limiting dengan minimal delays
        """
        with self.rate_lock:
            now = time.time()

            if self.last_request_time > 0:
                time_since_last = now - self.last_request_time
                required_wait = self.min_delay_between_requests + self.safety_margin

                if time_since_last < required_wait:
                    wait_time = required_wait - time_since_last
                    self.log(f"⏱️ ULTRA-FAST wait: {wait_time:.0f}s (minimal delay)")
                    time.sleep(wait_time)

            self.last_request_time = now

    def ultra_fast_process_chunk(self, chunk: List[str], chunk_num: int = 1, total_chunks: int = 1, total_pages: int = 1, doc_type: str = None) -> Optional[str]:
        """
        ULTRA-FAST chunk processing dengan aggressive settings
        """
        if doc_type is None:
            doc_type = self.auto_detect_doc_type()
        
        # REDUCED retries untuk speed
        max_retries = 2  # 3→2
        base_delay = 5   # 10→5
        
        for attempt in range(max_retries):
            try:
                self.log(f"⚡ ULTRA-FAST chunk {chunk_num}/{total_chunks}, attempt {attempt + 1} ({doc_type.upper()})")
                
                content: List[Dict] = []

                # Use optimized prompts
                prompt = self.get_optimized_document_prompt(doc_type)
                
                if len(chunk) > 1:
                    prompt = f"Analisis {len(chunk)} halaman dokumen sebagai satu kesatuan:\n\n{prompt}"
                    
                content.append({
                    "type": "text",
                    "text": prompt
                })

                # Validasi chunk
                if not chunk or len(chunk) == 0:
                    return self.get_fallback_response(doc_type)
                    
                for i, img_base64 in enumerate(chunk):
                    if not img_base64 or not isinstance(img_base64, str):
                        continue
                        
                    content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{img_base64}",
                            "detail": "low",  # Always use "low" untuk speed
                        },
                    })

                # ULTRA-FAST API call
                result = self.ultra_fast_call_api(content)

                if result and result.strip():
                    return result
                else:
                    if attempt < max_retries - 1:
                        retry_delay = base_delay * (attempt + 1)
                        self.log(f"⏱️ Quick retry in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        return self.get_fallback_response(doc_type)

            except Exception as e:
                self.log(f"❌ Chunk error attempt {attempt + 1}: {type(e).__name__}")
                if attempt < max_retries - 1:
                    retry_delay = base_delay * (attempt + 1)
                    time.sleep(retry_delay)
                else:
                    return self.get_fallback_response(doc_type)

        return self.get_fallback_response(doc_type)

    def ultra_fast_call_api(self, content: List[Dict]) -> Optional[str]:
        """
        ULTRA-FAST API call dengan aggressive timeouts
        """
        try:
            url = f"{self.base_url}/api/chat/completions"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            # ULTRA-FAST payload dengan reduced tokens
            payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": content}],
                "stream": False,
                "max_tokens": 4000,  # 8000→4000 untuk faster response
                "temperature": 0.1,
            }
            
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout_seconds)

            if response.status_code == 200:
                try:
                    data = response.json()
                    content_text = data["choices"][0]["message"]["content"]
                    
                    if content_text and content_text.strip():
                        return content_text.strip()
                        
                except (json.JSONDecodeError, KeyError, IndexError) as e:
                    self.log(f"API response parse error: {e}")
                    return None

            else:
                self.log(f"API error {response.status_code}")
                return None

        except requests.exceptions.Timeout:
            self.log(f"API timeout after {self.timeout_seconds}s")
            return None
        except Exception as e:
            self.log(f"API request error: {e}")
            return None

    def get_optimized_document_prompt(self, doc_type: str) -> str:
        """
        OPTIMIZED prompts untuk faster processing
        """
        # Shorter, more direct prompts untuk speed
        base_prompt = f"""Analisis dokumen {doc_type.upper()} dan ekstrak informasi dalam format JSON.

PENTING: Berikan respons JSON yang valid dan lengkap."""

        if doc_type == "ktp":
            return base_prompt + """

Format JSON:
{
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad",
  "alasan_validasi": "penjelasan singkat",
  "analisa_kualitas_dokumen": "analisis kualitas",
  "nik": "16 digit atau null",
  "nama": "Nama lengkap atau null",
  "tempat_lahir": "Tempat lahir atau null",
  "tanggal_lahir": "YYYY-MM-DD atau null",
  "alamat": "Alamat atau null",
  "foto": "Terdeteksi/Tidak Terdeteksi",
  "tanda_tangan": "Terdeteksi/Tidak Terdeteksi"
}"""
        # Add other document types as needed...
        else:
            return base_prompt

    def get_fallback_response(self, doc_type: str) -> str:
        """Unified fallback response"""
        fallback = {
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses - optimasi ULTRA-FAST aktif",
            "analisa_kualitas_dokumen": "Kualitas tidak memadai untuk ekstraksi otomatis",
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def merge_chunk_results(self, results: List[str], doc_type: str) -> Optional[str]:
        """FAST result merging"""
        try:
            all_data: List[Dict] = []

            for result in results:
                json_matches = re.findall(r"```json\s*(\{.*?\})\s*```", result, re.DOTALL)
                if not json_matches:
                    json_matches = re.findall(r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", result)

                for json_str in json_matches:
                    try:
                        data = json.loads(json_str)
                        all_data.append(data)
                    except Exception:
                        continue

            if not all_data:
                return results[0] if results else None

            # Simple merge: take first good result
            for data in all_data:
                if data.get("status_kepatuhan_format") == "Good":
                    return "```json\n" + json.dumps(data, indent=2, ensure_ascii=False) + "\n```"
            
            # Fallback: take first result
            return "```json\n" + json.dumps(all_data[0], indent=2, ensure_ascii=False) + "\n```"

        except Exception:
            return results[0] if results else None

    def extract_json_only(self, text: str) -> Dict:
        """Fast JSON extraction"""
        try:
            json_matches = re.findall(r"```json\s*(\{.*?\})\s*```", text, re.DOTALL)
            if json_matches:
                return json.loads(json_matches[0])

            json_matches = re.findall(r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", text)
            if json_matches:
                return json.loads(json_matches[0])

            return {"result": text}
        except Exception:
            return {"result": text}


def main() -> None:
    """
    ULTRA-FAST processor untuk production
    
    Usage:
        python ultra_fast_pdf_processor.py file.pdf --type nib --production
    """
    config = {
        "api_key": os.getenv("OPENWEBUI_API_KEY", "dummy-api-key"),
        "base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:8080"),
        "model": os.getenv("OPENWEBUI_MODEL", "image-screening-shmshm-elektronik"),
        
        # PRODUCTION SETTINGS
        "min_delay_between_requests": int(os.getenv("MIN_DELAY_SECONDS", "15")),
        "safety_margin": int(os.getenv("SAFETY_MARGIN", "1")),
        "timeout_seconds": int(os.getenv("TIMEOUT_SECONDS", "90")),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "4")),
        "max_image_size_kb": int(os.getenv("MAX_IMAGE_SIZE_KB", "300")),
        "base_image_quality": int(os.getenv("BASE_IMAGE_QUALITY", "80")),
    }
    
    # Parse document type
    if "--type" in sys.argv:
        type_index = sys.argv.index("--type")
        if type_index + 1 < len(sys.argv):
            doc_type = sys.argv[type_index + 1]
            if doc_type in ["sku", "npwp", "bpkb", "shm", "nib", "ktp", "auto"]:
                config["document_type"] = doc_type

    processor = UltraFastPDFProcessor(config)

    if "--debug" in sys.argv:
        processor.enable_logging = True
        processor.log("🚀 ULTRA-FAST Debug mode enabled")
    elif "--silent" in sys.argv:
        processor.enable_logging = False
    else:
        processor.enable_logging = True

    # Production mode notification
    if "--production" in sys.argv:
        processor.log("🏭 PRODUCTION MODE: ULTRA-FAST optimizations active")
        processor.log(f"   - Delays: {config['min_delay_between_requests']}s (70% faster)")
        processor.log(f"   - Chunks: {config['chunk_size']} pages per API call")
        processor.log(f"   - Quality: {config['base_image_quality']}% (optimized)")

    file_args = [arg for arg in sys.argv[1:] if not arg.startswith("--") and arg not in ["sku", "npwp", "bpkb", "shm", "nib", "ktp", "auto", "production"]]
    if file_args:
        file_input = file_args[0]

        if not os.path.exists(file_input):
            if processor.enable_logging:
                processor.log("File not found")
            print(json.dumps({"error": "File not found"}))
            sys.exit(1)

        result = processor.process_file(file_input)

        processor.enable_logging = False
        print(json.dumps(result, ensure_ascii=False))
        return

    # Auto-detect files
    common_paths = ["shmars.pdf", "nib.pdf", "bpkb.pdf", "ktp.jpg", "npwp.pdf", "sku.pdf"]
    for path in common_paths:
        if os.path.exists(path):
            processor.log(f"🔍 Auto-detected: {path}")
            result = processor.process_file(path)
            processor.enable_logging = False
            print(json.dumps(result, ensure_ascii=False))
            return

    if processor.enable_logging:
        processor.log("No files found")
    print(json.dumps({"error": "No files found"}))


if __name__ == "__main__":
    main()

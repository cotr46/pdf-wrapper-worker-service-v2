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


class SilentPDFProcessor:
    """Processor dengan internal logging tapi output akhir tetap bersih (JSON saja) - OPTIMIZED VERSION."""

    def __init__(self, config: Dict):
        self.api_key = config["api_key"]
        self.base_url = config["base_url"]
        self.model = config["model"]
        
        # TAMBAHAN: Document type detection
        self.document_type = config.get("document_type", "auto")  # "bpkb", "shm", "nib", or "auto"

        # Setting chunk & image - OPTIMIZED
        self.chunk_size = 2
        self.max_image_size_kb = 400
        self.base_image_quality = 88
        self.timeout_seconds = 150

        self.min_delay_between_requests = 70
        self.safety_margin = 5

        self.last_request_time = 0.0
        self.rate_lock = threading.Lock()

        self.enable_logging = True
        self.log_messages: List[str] = []
        
        # CLOUD RUN OPTIMIZATIONS
        self._optimize_for_cloud_run()

    def _optimize_for_cloud_run(self):
        """
        Cloud Run specific optimizations
        """
        import os
        
        # Limit threads untuk serverless environment
        os.environ['OPENBLAS_NUM_THREADS'] = '2'
        os.environ['MKL_NUM_THREADS'] = '2'
        os.environ['OMP_NUM_THREADS'] = '2'
        
        # PIL optimizations
        from PIL import Image
        Image.MAX_IMAGE_PIXELS = 100000000  # Prevent huge images
        Image.LOAD_TRUNCATED_IMAGES = True  # Handle truncated files
        
        self.log("Cloud Run optimizations applied")

    def log(self, message: str) -> None:
        """Logging internal yang bisa dimatikan."""
        if not self.enable_logging:
            return
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        print(log_msg, file=sys.stderr, flush=True)
        self.log_messages.append(log_msg)

    def fast_rotate_if_needed(self, pil_image: Image.Image) -> Image.Image:
        """
        OPTIMIZED ROTATION - 50x lebih cepat dari rotate(90, expand=True)
        """
        width, height = pil_image.size
        aspect_ratio = height / width
        
        # Hanya rotate jika benar-benar portrait (lebih ketat dari sebelumnya)
        if aspect_ratio > 1.4:  # Dari 1.0 ke 1.4 untuk mengurangi false positive
            # transpose 50x lebih cepat dari rotate()
            return pil_image.transpose(Image.Transpose.ROTATE_90)
        return pil_image

    def auto_detect_doc_type(self, file_path: str = None) -> str:
        """
        Auto-detect document type dari berbagai sumber
        """
        # 1. Cek self.document_type jika sudah di-set
        if hasattr(self, 'document_type') and self.document_type and self.document_type != "auto":
            return self.document_type
        
        # 2. Cek dari file path jika ada
        if file_path:
            filename = os.path.basename(file_path).lower()
            if "bpkb" in filename:
                return "bpkb"
            elif "shm" in filename or "sertifikat" in filename:
                return "shm"
            elif "nib" in filename:
                return "nib"
            elif "ktp" in filename:
                return "ktp"
            elif "npwp" in filename:
                return "npwp"
            elif "sku" in filename:
                return "sku"
        
        # 3. Cek dari nama model
        model_lower = self.model.lower()
        if "bpkb" in model_lower:
            return "bpkb"
        elif "shm" in model_lower:
            return "shm"
        elif "nib" in model_lower:
            return "nib"
        elif "ktp" in model_lower:
            return "ktp"
        elif "npwp" in model_lower:
            return "npwp"
        elif "sku" in model_lower:
            return "sku"
        
        # 4. Default fallback berdasarkan model name pattern
        return "nib"

    def process_file(self, file_path: str) -> Dict:
        """
        Entry point yang bisa handle PDF dan image files
        """
        try:
            # Deteksi tipe file berdasarkan ekstensi
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.pdf':
                self.log("Processing as PDF file")
                return self.process_pdf(file_path)
            elif file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                self.log("Processing as image file")
                return self.process_image_optimized(file_path)
            else:
                return {"error": f"Unsupported file type: {file_ext}"}
                
        except Exception as e:
            self.log(f"File processing error: {type(e).__name__}: {e}")
            return {
                "error": f"File processing error: {str(e)}",
                "error_type": type(e).__name__
            }
        finally:
            # Cleanup after processing
            self._cleanup_memory()

    def process_image_optimized(self, image_path: str) -> Dict:
        """
        OPTIMIZED process_image - Mengganti process_image yang lama
        """
        start_time = time.time()

        try:
            self.log(f"Starting OPTIMIZED image processing: {os.path.basename(image_path)}")

            if not self.validate_image_file(image_path):
                self.log("Image validation failed")
                return {"error": "Image validation failed"}

            detected_type = self.auto_detect_doc_type(image_path)
            self.log(f"Detected document type: {detected_type}")

            self.log("Converting image to base64...")
            try:
                # Direct PIL loading (lebih efisien dari read + BytesIO)
                pil_image = Image.open(image_path)
                pil_image.load()  # Load immediately
            
                # Convert mode efficiently
                if pil_image.mode not in ("RGB", "L"):
                    pil_image = pil_image.convert("RGB")

                # FAST ROTATION - ini yang dioptimasi!
                pil_image = self.fast_rotate_if_needed(pil_image)

                # Fast optimization tanpa enhancement berat
                optimized_base64 = self.optimize_image_fast(pil_image)
                
                # Cleanup immediately
                pil_image.close()
                del pil_image
                
                if not optimized_base64:
                    self.log("Image optimization failed")
                    return {"error": "Image optimization failed"}
                    
                self.log("Image converted successfully with FAST method")

            except Exception as e:
                self.log(f"Image conversion error: {e}")
                return {"error": f"Image conversion failed: {str(e)}"}

            chunk = [optimized_base64]
            self.wait_ultra_safe()
            
            result = self.process_chunk(chunk, 1, 1, 1, detected_type)
            
            if result and result.strip():
                self.log(f"Image processing SUCCESS ({len(result)} chars)")
                json_result = self.extract_json_only(result)
                
                processing_time = time.time() - start_time
                self.log(f"OPTIMIZED processing completed in {processing_time:.1f}s")
                
                return json_result
            else:
                self.log("Image processing FAILED - using fallback")
                fallback_response = self.get_fallback_response(detected_type)
                processing_time = time.time() - start_time
                self.log(f"Processing completed with fallback in {processing_time:.1f}s")
                return self.extract_json_only(fallback_response)

        except Exception as e:
            self.log(f"Image processing error: {type(e).__name__}: {e}")
            processing_time = time.time() - start_time
            self.log(f"Processing failed after {processing_time:.1f}s")
            
            detected_type = self.auto_detect_doc_type(image_path)
            fallback_response = self.get_fallback_response(detected_type)
            return self.extract_json_only(fallback_response)

    def validate_image_file(self, image_path: str) -> bool:
        """
        Validasi file image
        """
        try:
            if not os.path.exists(image_path):
                self.log("Image file not found")
                return False

            # Cek ekstensi file
            valid_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
            file_ext = os.path.splitext(image_path)[1].lower()
            if file_ext not in valid_extensions:
                self.log(f"Invalid image extension: {file_ext}")
                return False

            # Cek ukuran file
            file_size_mb = os.path.getsize(image_path) / (1024 * 1024)
            if file_size_mb > 50:  # Max 50MB
                self.log(f"Image file too large: {file_size_mb:.1f}MB")
                return False

            # Test buka image dengan PIL
            try:
                with Image.open(image_path) as img:
                    img.verify()
                self.log(f"Image validated: {file_size_mb:.1f}MB")
                return True
            except Exception as e:
                self.log(f"Image validation failed: {e}")
                return False

        except Exception as e:
            self.log(f"Image validation error: {e}")
            return False

    def process_pdf(self, file_path: str) -> Dict:
        """
        Proses file PDF dengan optimasi rotation
        """
        start_time = time.time()

        try:
            self.log(f"Starting OPTIMIZED PDF processing: {os.path.basename(file_path)}")

            if not self.validate_file(file_path):
                self.log("PDF validation failed")
                return {"error": "PDF validation failed"}

            # Deteksi tipe dokumen
            detected_type = self.auto_detect_doc_type(file_path)
            self.log(f"Detected document type: {detected_type}")

            self.log("Converting PDF to chunks with FAST method...")
            chunks_data = self.convert_pdf_optimized(file_path)
            if not chunks_data:
                self.log("PDF conversion failed")
                return {"error": "PDF conversion failed"}

            chunks = chunks_data["chunks"]
            total_pages = chunks_data["total_pages"]
            self.log(f"Created {len(chunks)} chunks from {total_pages} pages using FAST method")

            results: List[str] = []
            failed_chunks = 0

            for i, chunk in enumerate(chunks):
                self.log(f"Processing chunk {i + 1}/{len(chunks)} ({len(chunk)} images)...")

                self.wait_ultra_safe()

                result = self.process_chunk(chunk, i + 1, len(chunks), total_pages, detected_type)
                if result and result.strip():
                    results.append(result)
                    self.log(f"Chunk {i + 1}: SUCCESS ({len(result)} chars)")
                else:
                    failed_chunks += 1
                    self.log(f"Chunk {i + 1}: FAILED")

            self.log("Combining results...")
            if not results:
                self.log("All chunks failed, returning fallback response")
                
                fallback_response = self.get_fallback_response(detected_type)
                processing_time = time.time() - start_time
                self.log(f"PDF processing completed with fallback in {processing_time:.1f}s")
                return self.extract_json_only(fallback_response)

            if len(results) == 1:
                final_result = results[0]
            else:
                final_result = self.merge_chunk_results(results, detected_type)
                if not final_result:
                    self.log("Merge failed, using first result")
                    final_result = results[0]

            json_result = self.extract_json_only(final_result)

            processing_time = time.time() - start_time
            self.log(f"OPTIMIZED PDF processing completed in {processing_time:.1f}s")
            self.log(f"Success rate: {len(results)}/{len(chunks)} chunks, {failed_chunks} failed")

            return json_result

        except Exception as e:
            self.log(f"PDF processing error: {type(e).__name__}: {e}")
            processing_time = time.time() - start_time
            self.log(f"PDF processing failed after {processing_time:.1f}s")
            
            detected_type = self.auto_detect_doc_type(file_path)
            fallback_response = self.get_fallback_response(detected_type)
            return self.extract_json_only(fallback_response)

    def convert_pdf_optimized(self, pdf_path: str) -> Optional[Dict]:
        """
        OPTIMIZED PDF conversion dengan fast rotation
        """
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            self.log(f"Failed to open PDF: {str(e)}")
            return None

        try:
            total_pages = doc.page_count
        except Exception as e:
            self.log(f"Failed to read page count: {str(e)}")
            doc.close()
            return None

        # Optimized settings untuk Cloud Run
        dpi = 120  # Turunkan dari 150 untuk speed
        zoom = dpi / 72.0
        matrix = fitz.Matrix(zoom, zoom)

        base64_images: List[str] = []

        self.log(f"PDF has {total_pages} pages, starting OPTIMIZED conversion...")

        for page_num in range(total_pages):
            page_start_time = time.time()
            
            try:
                page = doc.load_page(page_num)
                
                # Direct JPEG conversion - skip PNG untuk speed
                pixmap = page.get_pixmap(matrix=matrix, alpha=False)

                try:
                    # Direct JPEG - lebih cepat dari PNG
                    img_bytes = pixmap.tobytes("jpeg", jpg_quality=85)
                except Exception as e:
                    self.log(f"  Page {page_num + 1}: JPEG conversion failed: {str(e)[:80]}, using fallback...")
                    img_bytes = pixmap.tobytes()

                # Immediate cleanup
                pixmap = None

                # Load PIL dari JPEG bytes
                pil_image = Image.open(BytesIO(img_bytes))
                
                # Cleanup bytes immediately
                del img_bytes

                # Normalize mode efficiently
                if pil_image.mode not in ("RGB", "L"):
                    pil_image = pil_image.convert("RGB")

                # FAST ROTATION - ini yang dioptimasi!
                pil_image = self.fast_rotate_if_needed(pil_image)

                # SKIP enhancement untuk speed
                # enhancer = ImageEnhance.Contrast(pil_image)
                # pil_image = enhancer.enhance(1.1)

                # Fast optimization
                optimized_base64 = self.optimize_image_fast(pil_image)

                if optimized_base64:
                    base64_images.append(optimized_base64)
                    page_time = time.time() - page_start_time
                    self.log(f"  Page {page_num + 1}: Converted in {page_time:.2f}s (FAST)")
                else:
                    self.log(f"  Page {page_num + 1}: Optimization failed")
                
                # Force cleanup
                pil_image.close()
                del pil_image

            except Exception as e:
                self.log(f"  Page {page_num + 1}: Conversion error - {str(e)[:120]}")
                continue
            
            # Force garbage collection every few pages
            if page_num % 3 == 0:
                gc.collect()

        try:
            doc.close()
        except Exception:
            pass

        # Final cleanup
        gc.collect()

        if not base64_images:
            self.log("No pages could be converted")
            return None

        chunks: List[List[str]] = [
            base64_images[i: i + self.chunk_size]
            for i in range(0, len(base64_images), self.chunk_size)
        ]

        self.log(f"OPTIMIZED conversion: {len(base64_images)}/{total_pages} pages in {len(chunks)} chunks")

        return {
            "chunks": chunks,
            "total_pages": total_pages,
        }

    def optimize_image_fast(self, pil_image: Image.Image) -> Optional[str]:
        """
        FAST image optimization tanpa quality loop dan enhancement berat
        """
        try:
            if pil_image.mode not in ("RGB", "L"):
                pil_image = pil_image.convert("RGB")

            # Smaller max dimension untuk speed
            max_dimension = 1400  # Turun dari 2000

            # Resize jika perlu
            if max(pil_image.width, pil_image.height) > max_dimension:
                if pil_image.width >= pil_image.height:
                    ratio = max_dimension / pil_image.width
                    new_size = (max_dimension, int(pil_image.height * ratio))
                else:
                    ratio = max_dimension / pil_image.height
                    new_size = (int(pil_image.width * ratio), max_dimension)
                
                pil_image = pil_image.resize(new_size, Image.Resampling.LANCZOS)

            # SKIP enhancement loops untuk speed
            # TIDAK ada: contrast_enhancer, sharpness_enhancer, quality loops
            
            # Direct JPEG encoding dengan fixed quality
            buffer = BytesIO()
            quality = 82  # Fixed quality untuk speed
            
            pil_image.save(buffer, format="JPEG", quality=quality, optimize=True)
            data = buffer.getvalue()
            size_kb = len(data) / 1024

            # Jika masih terlalu besar, quick resize
            if size_kb > 400:
                new_w = int(pil_image.width * 0.8)
                new_h = int(pil_image.height * 0.8)
                pil_image = pil_image.resize((new_w, new_h), Image.Resampling.LANCZOS)
                
                buffer = BytesIO()
                pil_image.save(buffer, format="JPEG", quality=75, optimize=True)
                data = buffer.getvalue()
                size_kb = len(data) / 1024

            self.log(f"Fast optimization: {size_kb:.1f}KB")
            return base64.b64encode(data).decode("utf-8")

        except Exception as e:
            self.log(f"Fast optimization error: {e}")
            return None

    def _cleanup_memory(self):
        """
        Force memory cleanup untuk Cloud Run
        """
        gc.collect()

    def process_chunk(self, chunk: List[str], chunk_num: int = 1, total_chunks: int = 1, total_pages: int = 1, doc_type: str = None) -> Optional[str]:
        """
        BACKWARD COMPATIBLE: Semua parameter opsional kecuali chunk
        """
        # Auto-detect doc_type jika tidak diberikan
        if doc_type is None:
            doc_type = self.auto_detect_doc_type()
            self.log(f"No doc_type provided, auto-detected: {doc_type}")
        
        max_retries = 3
        base_delay = 10
        
        for attempt in range(max_retries):
            try:
                self.log(f"Processing chunk {chunk_num}/{total_chunks}, attempt {attempt + 1}/{max_retries} for {doc_type.upper()}")
                
                content: List[Dict] = []

                # Gunakan prompt yang sesuai dengan tipe dokumen
                prompt = self.get_document_prompt(doc_type)
                
                if len(chunk) > 1:
                    prompt = f"Analisis {len(chunk)} halaman dokumen berikut sebagai satu kesatuan:\n\n{prompt}"
                
                # Untuk NIB, tambahkan instruksi khusus jika retry
                if doc_type == "nib" and attempt > 0:
                    prompt += f"\n\nPERCOBAANG KE-{attempt + 1}: Jika gambar tidak jelas, berikan analisis terbaik yang bisa dilakukan dan isi field dengan 'Tidak dapat dibaca' jika benar-benar tidak terlihat. WAJIB memberikan respons JSON yang valid."
                    
                content.append({
                    "type": "text",
                    "text": prompt
                })

                # Validasi chunk sebelum diproses
                if not chunk or len(chunk) == 0:
                    self.log("Error: Chunk is empty")
                    return self.get_fallback_response(doc_type)
                    
                for i, img_base64 in enumerate(chunk):
                    if not img_base64 or not isinstance(img_base64, str):
                        self.log(f"Error: Image {i+1} in chunk is invalid")
                        continue
                        
                    content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/jpeg;base64,{img_base64}",
                            "detail": "high" if doc_type == "nib" else "low",
                        },
                    })

                self.log(f"Sending API request with {len(chunk)} images for {doc_type.upper()} (attempt {attempt + 1})...")
                
                # Timeout yang lebih panjang untuk NIB
                original_timeout = self.timeout_seconds
                if doc_type == "nib":
                    self.timeout_seconds = 200
                
                result = self.call_api(content)
                
                # Restore timeout
                self.timeout_seconds = original_timeout

                if result and result.strip():
                    # Validasi respons untuk NIB
                    if doc_type == "nib":
                        if not self.validate_nib_response(result):
                            self.log(f"NIB response validation failed on attempt {attempt + 1}")
                            if attempt < max_retries - 1:
                                retry_delay = base_delay * (attempt + 1)
                                self.log(f"Waiting {retry_delay}s before retry...")
                                time.sleep(retry_delay)
                                continue
                            else:
                                self.log("NIB validation failed on final attempt, using fallback")
                                return self.get_fallback_response(doc_type)
                    
                    self.log(f"API response received: {len(result)} characters")
                    return result
                else:
                    self.log(f"Empty or None response received on attempt {attempt + 1}")
                    
                    if attempt < max_retries - 1:
                        retry_delay = base_delay * (attempt + 1)
                        self.log(f"Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        self.log("Max retries reached, using fallback response")
                        return self.get_fallback_response(doc_type)

            except Exception as e:
                self.log(f"Chunk processing error on attempt {attempt + 1}: {type(e).__name__}: {e}")
                if attempt < max_retries - 1:
                    retry_delay = base_delay * (attempt + 1)
                    self.log(f"Exception occurred, waiting {retry_delay}s before retry...")
                    time.sleep(retry_delay)
                else:
                    self.log("All attempts failed due to exceptions, using fallback")
                    return self.get_fallback_response(doc_type)

        self.log("All processing attempts failed")
        return self.get_fallback_response(doc_type)

    def get_fallback_response(self, doc_type: str) -> str:
        """
        Unified fallback response berdasarkan doc type
        """
        if doc_type == "sku":
            return self.get_sku_fallback_response()
        elif doc_type == "npwp":
            return self.get_npwp_fallback_response()
        elif doc_type == "bpkb":
            return self.get_bpkb_fallback_response()
        elif doc_type == "shm":
            return self.get_shm_fallback_response()
        elif doc_type == "nib":
            return self.get_nib_fallback_response()
        elif doc_type == "ktp":
            return self.get_ktp_fallback_response()
        else:
            return self.get_nib_fallback_response()  # default

    def get_sku_fallback_response(self) -> str:
        """
        Respons fallback untuk SKU jika API gagal total
        """
        self.log("Generating SKU fallback response")
        fallback = {
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses oleh sistem AI - kemungkinan kualitas gambar buruk atau format tidak standar",
            "analisa_kualitas_dokumen": "Kualitas dokumen tidak memadai untuk ekstraksi otomatis",
            "nomor_surat": None,
            "nama": None,
            "tanggal": None,
            "kelurahan_desa": None,
            "kecamatan": None
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def get_npwp_fallback_response(self) -> str:
        """
        Respons fallback untuk NPWP jika API gagal total
        """
        self.log("Generating NPWP fallback response")
        fallback = {
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses oleh sistem AI - kemungkinan kualitas gambar buruk atau format tidak standar",
            "analisa_kualitas_dokumen": "Kualitas dokumen tidak memadai untuk ekstraksi otomatis, tidak dapat menentukan versi NPWP",
            "npwp": None,
            "nama_wajib_pajak": None,
            "alamat": None,
            "kpp": None,
            "tanggal_terbit": None,
            "status_pusat_cabang": None,
            "logo_djp": "Tidak Terdeteksi"
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def get_bpkb_fallback_response(self) -> str:
        """
        Respons fallback untuk BPKB jika API gagal total
        """
        self.log("Generating BPKB fallback response")
        fallback = {
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses oleh sistem AI - kemungkinan kualitas gambar buruk atau format tidak standar",
            "analisa_kualitas_dokumen": "Kualitas dokumen tidak memadai untuk ekstraksi otomatis, halaman penting tidak dapat dibaca",
            "halaman_identitas_pemilik": {
                "nomor_bpkb": None,
                "nama_pemilik": None,
                "nik": None
            },
            "halaman_identitas_kendaraan": {
                "nomor_registrasi": None,
                "merek": None,
                "tipe": None,
                "warna": None
            }
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def get_shm_fallback_response(self) -> str:
        """
        Respons fallback untuk SHM jika API gagal total
        """
        self.log("Generating SHM fallback response")
        fallback = {
            "tipe_dokumen": "Sertifikat Hak Milik",
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses oleh sistem AI - kemungkinan kualitas gambar buruk atau format tidak standar",
            "analisa_kualitas_dokumen": "Kualitas dokumen tidak memadai untuk ekstraksi otomatis",
            "halaman_1": {
                "jenis_hak": None,
                "nomor_sertifikat": None,
                "nib": None,
                "kode_sertifikat": None,
                "pemegang_hak": None,
                "luas": None,
                "kelurahan": None,
                "kecamatan": None,
                "kota": None
            },
            "halaman_2": {
                "tanggal_penerbitan": None,
                "luas_nib": None,
                "letak_tanah": None
            },
            "surat_ukur": {
                "nomor_surat_ukur": None,
                "luas": None,
                "kelurahan": None,
                "kecamatan": None,
                "kota": None
            }
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def get_document_prompt(self, doc_type: str) -> str:
        """
        Generate prompt yang sesuai dengan tipe dokumen
        """
        if doc_type == "sku":
            return """Analisis dokumen SKU (Surat Keterangan Usaha) berikut dan ekstrak informasi dalam format JSON.

PENTING: 
- Baca semua teks yang terlihat dengan teliti
- Jika ada field yang tidak ditemukan, isi dengan null
- Pastikan memberikan respons JSON yang valid

Format JSON yang diharapkan:
{
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad",
  "alasan_validasi": "penjelasan singkat hasil pemeriksaan",
  "analisa_kualitas_dokumen": "analisis kualitas (resolusi, warna, OCR, dll)",
  "nomor_surat": "Nomor SKU atau null",
  "nama": "Nama pemilik usaha atau nama usaha atau null",
  "tanggal": "YYYY-MM-DD atau null",
  "kelurahan_desa": "Kelurahan/Desa atau null",
  "kecamatan": "Kecamatan atau null"
}

Pastikan untuk menganalisis seluruh dokumen SKU dengan teliti."""

        elif doc_type == "npwp":
            return """Analisis dokumen NPWP (Nomor Pokok Wajib Pajak) berikut dan ekstrak informasi dalam format JSON.

PENTING: 
- Baca semua teks yang terlihat dengan teliti
- Identifikasi apakah NPWP versi baru atau lama
- Jika ada field yang tidak ditemukan, isi dengan null
- Pastikan memberikan respons JSON yang valid

Format JSON yang diharapkan:
{
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad",
  "alasan_validasi": "penjelasan singkat hasil pemeriksaan",
  "analisa_kualitas_dokumen": "analisis kualitas (resolusi, warna, OCR, dll), sebutkan apakah NPWP versi baru atau lama",
  "npwp": "15 digit sesuai format atau null",
  "nama_wajib_pajak": "Nama lengkap wajib pajak atau null",
  "alamat": "Alamat wajib pajak atau null",
  "kpp": "Nama KPP atau null",
  "tanggal_terbit": "YYYY-MM-DD atau null",
  "status_pusat_cabang": "Pusat/Cabang/null",
  "logo_djp": "Terdeteksi/Tidak Terdeteksi"
}

Pastikan untuk menganalisis seluruh dokumen NPWP dengan teliti."""

        elif doc_type == "bpkb":
            return """Analisis dokumen BPKB (Buku Pemilik Kendaraan Bermotor) berikut dan ekstrak informasi dalam format JSON.

PENTING: 
- Baca semua teks yang terlihat dengan teliti
- Analisis halaman identitas pemilik dan identitas kendaraan
- Jika ada field yang tidak ditemukan, isi dengan null
- Pastikan memberikan respons JSON yang valid

Format JSON yang diharapkan:
{
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad",
  "alasan_validasi": "penjelasan singkat hasil pemeriksaan",
  "analisa_kualitas_dokumen": "analisis kualitas (resolusi, warna, OCR, halaman penting, dll)",
  "halaman_identitas_pemilik": {
    "nomor_bpkb": "Nomor BPKB atau null",
    "nama_pemilik": "Nama pemilik atau null",
    "nik": "NIK atau null"
  },
  "halaman_identitas_kendaraan": {
    "nomor_registrasi": "Nomor polisi atau null",
    "merek": "Merek kendaraan atau null",
    "tipe": "Tipe kendaraan atau null",
    "warna": "Warna kendaraan atau null"
  }
}

Pastikan untuk menganalisis seluruh halaman BPKB dengan teliti."""

        elif doc_type == "shm":
            return """Analisis dokumen Sertifikat Hak Milik (SHM) berikut dan ekstrak informasi dalam format JSON.

PENTING: 
- Baca semua teks yang terlihat dengan teliti
- Jika ada field yang tidak ditemukan, isi dengan null
- Pastikan memberikan respons JSON yang valid

Format JSON yang diharapkan:
{
  "tipe_dokumen": "Sertifikat Hak Milik",
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad", 
  "alasan_validasi": "penjelasan singkat hasil pemeriksaan",
  "analisa_kualitas_dokumen": "analisis kualitas (resolusi, warna, OCR, dll)",
  "halaman_1": {
    "jenis_hak": "jenis hak tanah atau null",
    "nomor_sertifikat": "nomor sertifikat atau null",
    "nib": "NIB atau null",
    "kode_sertifikat": "kode sertifikat atau null",
    "pemegang_hak": "nama pemegang hak atau null",
    "luas": "luas tanah atau null",
    "kelurahan": "kelurahan/desa atau null",
    "kecamatan": "kecamatan atau null", 
    "kota": "kota/kabupaten atau null"
  },
  "halaman_2": {
    "tanggal_penerbitan": "tanggal penerbitan atau null",
    "luas_nib": "luas berdasarkan NIB atau null",
    "letak_tanah": "deskripsi letak tanah atau null"
  },
  "surat_ukur": {
    "nomor_surat_ukur": "nomor surat ukur atau null",
    "luas": "luas berdasarkan surat ukur atau null", 
    "kelurahan": "kelurahan/desa atau null",
    "kecamatan": "kecamatan atau null",
    "kota": "kota/kabupaten atau null"
  }
}

Pastikan untuk menganalisis seluruh halaman SHM dengan teliti."""

        elif doc_type == "nib":
            return """Analisis dokumen NIB (Nomor Induk Berusaha) berikut dan ekstrak informasi dalam format JSON.

PENTING: 
- Baca semua teks yang terlihat dengan teliti
- Identifikasi apakah NIB tipe perorangan atau badan usaha
- Jika ada field yang tidak ditemukan, isi dengan null
- Pastikan memberikan respons JSON yang valid

Format JSON yang diharapkan:
{
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad",
  "alasan_validasi": "penjelasan singkat hasil pemeriksaan",
  "analisa_kualitas_dokumen": "analisis kualitas (resolusi, warna, OCR, dll), sebutkan tipe NIB: perorangan / badan usaha",
  "nib": "13 digit sesuai format atau null",
  "nama_usaha": "Nama usaha atau null",
  "pemilik": "Nama pemilik jika perorangan, null jika badan usaha",
  "status": "Perorangan/PMDN/Asing/dll/null",
  "alamat": "Alamat usaha atau null",
  "telepon": "Nomor telepon jika badan usaha, null lainnya",
  "email": "Email jika badan usaha, null lainnya",
  "kbli": "Kode KBLI (5 digit) atau null",
  "jenis_usaha": "Deskripsi KBLI atau sektor usaha/null",
  "skala_usaha": "Mikro/Kecil/Menengah/Besar/null",
  "izin_usaha": "Jenis izin usaha jika ada/null",
  "izin_komersial": "Jenis izin komersial jika ada/null",
  "status_penanaman_modal": "PMDN/PMA/null",
  "tanggal_terbit": "YYYY-MM-DD atau null",
  "qr_code": "Terdeteksi/Tidak Terdeteksi/null",
  "logo_oss_bkpm": "Terdeteksi/Tidak Terdeteksi"
}

Pastikan untuk menganalisis seluruh halaman NIB dengan teliti dan memberikan respons JSON yang valid."""

        elif doc_type == "ktp":
            return """Analisis dokumen KTP (Kartu Tanda Penduduk) berikut dan ekstrak informasi dalam format JSON.

PENTING: 
- Baca semua teks yang terlihat dengan teliti
- Periksa keberadaan foto dan tanda tangan
- Jika ada field yang tidak ditemukan, isi dengan null
- Pastikan memberikan respons JSON yang valid

Format JSON yang diharapkan:
{
  "status_kepatuhan_format": "Good | Recheck by Supervisor | Bad",
  "alasan_validasi": "penjelasan singkat tentang hasil verifikasi",
  "analisa_kualitas_dokumen": "analisis kualitas (resolusi, warna, OCR, dll)",
  "nik": "16 digit atau null",
  "nama": "Nama Lengkap atau null",
  "tempat_lahir": "Kota/Kabupaten atau null",
  "tanggal_lahir": "YYYY-MM-DD atau null",
  "jenis_kelamin": "Laki-laki/Perempuan/null",
  "golongan_darah": "A/B/AB/O/null",
  "alamat": "Alamat lengkap atau null",
  "rt_rw": "RT/RW atau null",
  "kel_desa": "Kel/Desa atau null",
  "kecamatan": "Kecamatan atau null",
  "agama": "Agama atau null",
  "status_perkawinan": "Belum Kawin/Kawin/Cerai/null",
  "pekerjaan": "Pekerjaan atau null",
  "kewarganegaraan": "WNI/WNA/null",
  "berlaku_hingga": "Tanggal atau Seumur Hidup/null",
  "provinsi": "Nama Provinsi/null",
  "kabupaten_kota": "Nama Kabupaten/Kota/null",
  "tanda_tangan": "Terdeteksi/Tidak Terdeteksi",
  "foto": "Terdeteksi/Tidak Terdeteksi"
}

Pastikan untuk menganalisis seluruh dokumen KTP dengan teliti."""

        else:
            # Auto detection prompt
            return """Analisis dokumen berikut. Tentukan apakah ini adalah SKU, NPWP, BPKB, SHM, NIB, atau KTP, kemudian ekstrak informasi yang sesuai.

PENTING: Berikan respons JSON yang valid dan lengkap sesuai format yang tepat untuk jenis dokumen yang terdeteksi.

Pastikan field status_kepatuhan_format, alasan_validasi, dan analisa_kualitas_dokumen selalu ada."""

    def validate_file(self, file_path: str) -> bool:
        """Validasi PDF sederhana dengan logging."""
        try:
            if not os.path.exists(file_path):
                self.log("File not found")
                return False

            doc = fitz.open(file_path)
            page_count = doc.page_count
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            doc.close()

            if page_count == 0:
                self.log("PDF has no pages")
                return False

            self.log(f"PDF validated: {page_count} pages, {file_size_mb:.1f}MB")
            return True

        except Exception as e:
            self.log(f"Validation error: {e}")
            return False

    def wait_ultra_safe(self) -> None:
        """Jeda super konservatif antar pemanggilan API."""
        with self.rate_lock:
            now = time.time()

            if self.last_request_time > 0:
                time_since_last = now - self.last_request_time
                required_wait = self.min_delay_between_requests + self.safety_margin

                if time_since_last < required_wait:
                    wait_time = required_wait - time_since_last
                    self.log(f"Rate limit protection: waiting {wait_time:.0f}s...")
                    time.sleep(wait_time)

            self.last_request_time = now

    def validate_nib_response(self, response: str) -> bool:
        """
        Validasi khusus untuk respons NIB dengan proper None checking
        """
        try:
            # Cek apakah response tidak None dan tidak kosong
            if not response or not isinstance(response, str):
                self.log("Response is None or not string")
                return False
                
            response_lower = response.lower()
            
            # Cek apakah ada JSON dalam respons
            if "```json" in response_lower or "{" in response_lower:
                # Cek apakah ada field NIB yang penting
                nib_keywords = ["nib", "nomor_induk", "perusahaan", "usaha", "berusaha"]
                if any(keyword in response_lower for keyword in nib_keywords):
                    self.log("NIB response validation passed")
                    return True
                else:
                    self.log("NIB response missing required keywords")
                    return False
            else:
                self.log("NIB response has no JSON structure")
                return False
        except Exception as e:
            self.log(f"NIB response validation error: {e}")
            return False

    def call_api(self, content: List[Dict]) -> Optional[str]:
        """
        Wrapper untuk endpoint /api/chat/completions OpenWebUI dengan enhanced error handling
        """
        try:
            url = f"{self.base_url}/api/chat/completions"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": content}],
                "stream": False,
                "max_tokens": 8000,  # Lebih besar untuk NIB yang kompleks
                "temperature": 0.1,
            }

            self.log(f"Making API call to {url} with model {self.model}")
            
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout_seconds)

            self.log(f"API response status: {response.status_code}")

            if response.status_code == 200:
                try:
                    # Cek response.text sebelum parse JSON
                    if not response.text or response.text.strip() == "":
                        self.log("API returned empty text response")
                        return None
                        
                    data = response.json()
                    self.log(f"API response parsed successfully")
                    
                    # Lebih robust checking untuk response structure
                    if not data:
                        self.log("API response data is None or empty")
                        return None
                        
                    if "choices" not in data:
                        self.log("No 'choices' field in API response")
                        self.log(f"Response structure: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
                        return None
                        
                    choices = data.get("choices")
                    if not choices or len(choices) == 0:
                        self.log("Choices array is empty")
                        return None
                        
                    choice = choices[0]
                    if not choice or not isinstance(choice, dict):
                        self.log("First choice is not a valid dict")
                        return None
                        
                    message = choice.get("message", {})
                    if not message:
                        self.log("No message in choice")
                        return None
                        
                    content_text = message.get("content")
                    
                    if content_text is None:
                        self.log("Content is None")
                        return None
                        
                    if not isinstance(content_text, str):
                        self.log(f"Content is not string, type: {type(content_text)}")
                        return None
                        
                    if content_text.strip() == "":
                        self.log("Content is empty string")
                        return None
                        
                    self.log(f"Valid content received: {len(content_text)} chars")
                    return content_text.strip()
                        
                except json.JSONDecodeError as e:
                    self.log(f"Failed to parse API response JSON: {e}")
                    self.log(f"Raw response: {response.text[:500]}...")
                    return None
                except Exception as e:
                    self.log(f"Error processing API response: {e}")
                    return None

            elif response.status_code == 429:
                self.log("Rate limited by API - will retry")
                return None
            elif response.status_code == 500:
                self.log(f"Server error - will retry. Response: {response.text[:200]}...")
                return None
            elif response.status_code == 502:
                self.log("Bad gateway - will retry")  
                return None
            elif response.status_code == 503:
                self.log("Service unavailable - will retry")
                return None
            else:
                self.log(f"API error {response.status_code}: {response.text[:200]}...")
                return None

        except requests.exceptions.Timeout:
            self.log(f"API request timeout after {self.timeout_seconds}s")
            return None
        except requests.exceptions.ConnectionError as e:
            self.log(f"Connection error to API: {e}")
            return None
        except Exception as e:
            self.log(f"Unexpected API request error: {e}")
            return None

    def get_nib_fallback_response(self) -> str:
        """
        Respons fallback untuk NIB jika API gagal total
        """
        self.log("Generating NIB fallback response")
        fallback = {
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses oleh sistem AI - kemungkinan kualitas gambar buruk atau format tidak standar",
            "analisa_kualitas_dokumen": "Kualitas dokumen tidak memadai untuk ekstraksi otomatis, tidak dapat menentukan tipe NIB",
            "nib": None,
            "nama_usaha": None,
            "pemilik": None,
            "status": None,
            "alamat": None,
            "telepon": None,
            "email": None,
            "kbli": None,
            "jenis_usaha": None,
            "skala_usaha": None,
            "izin_usaha": None,
            "izin_komersial": None,
            "status_penanaman_modal": None,
            "tanggal_terbit": None,
            "qr_code": "Tidak Terdeteksi",
            "logo_oss_bkpm": "Tidak Terdeteksi"
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def get_ktp_fallback_response(self) -> str:
        """
        Respons fallback untuk KTP jika API gagal total
        """
        self.log("Generating KTP fallback response")
        fallback = {
            "status_kepatuhan_format": "Bad",
            "alasan_validasi": "Dokumen tidak dapat diproses oleh sistem AI - kemungkinan kualitas gambar buruk atau format tidak standar",
            "analisa_kualitas_dokumen": "Kualitas dokumen tidak memadai untuk ekstraksi otomatis",
            "nik": None,
            "nama": None,
            "tempat_lahir": None,
            "tanggal_lahir": None,
            "jenis_kelamin": None,
            "golongan_darah": None,
            "alamat": None,
            "rt_rw": None,
            "kel_desa": None,
            "kecamatan": None,
            "agama": None,
            "status_perkawinan": None,
            "pekerjaan": None,
            "kewarganegaraan": None,
            "berlaku_hingga": None,
            "provinsi": None,
            "kabupaten_kota": None,
            "tanda_tangan": "Tidak Terdeteksi",
            "foto": "Tidak Terdeteksi"
        }
        
        return "```json\n" + json.dumps(fallback, indent=2, ensure_ascii=False) + "\n```"

    def merge_chunk_results(self, results: List[str], doc_type: str) -> Optional[str]:
        """
        Merge dengan struktur yang sesuai tipe dokumen
        Untuk kebanyakan dokumen (SKU, NPWP, KTP, NIB), biasanya single image/chunk
        """
        try:
            self.log("Merging multiple chunk results...")

            all_data: List[Dict] = []

            for i, result in enumerate(results):
                json_matches = re.findall(r"```json\s*(\{.*?\})\s*```", result, re.DOTALL)
                if not json_matches:
                    json_matches = re.findall(r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", result)

                for json_str in json_matches:
                    try:
                        data = json.loads(json_str)
                        all_data.append(data)
                        self.log(f"Extracted JSON from result {i + 1}")
                    except Exception:
                        continue

            if not all_data:
                self.log("No valid JSON found in results, using raw first result")
                return results[0] if results else None

            self.log(f"Merging {len(all_data)} JSON objects for {doc_type.upper()}...")

            # Untuk dokumen sederhana (SKU, NPWP, KTP, NIB), biasanya hanya perlu ambil hasil terbaik
            if doc_type in ["sku", "npwp", "ktp", "nib"]:
                # Ambil hasil dengan status terbaik atau yang paling lengkap
                best_result = self.get_best_simple_result(all_data)
                return "```json\n" + json.dumps(best_result, indent=2, ensure_ascii=False) + "\n```"
            elif doc_type == "bpkb":
                merged = self.merge_bpkb_data(all_data)
            elif doc_type == "shm":
                merged = self.merge_shm_data(all_data)
            else:
                # Default: ambil hasil pertama yang valid
                merged = all_data[0]

            self.log("Merge completed successfully")
            return "```json\n" + json.dumps(merged, indent=2, ensure_ascii=False) + "\n```"

        except Exception as e:
            self.log(f"Merge failed: {e}, using first result")
            return results[0] if results else None

    def get_best_simple_result(self, data_list: List[Dict]) -> Dict:
        """
        Untuk dokumen simple (SKU, NPWP, KTP, NIB), pilih hasil terbaik
        """
        if not data_list:
            return {}
            
        # Prioritas: Good > Recheck by Supervisor > Bad
        good_results = [d for d in data_list if d.get("status_kepatuhan_format") == "Good"]
        if good_results:
            return good_results[0]
            
        recheck_results = [d for d in data_list if d.get("status_kepatuhan_format") == "Recheck by Supervisor"]
        if recheck_results:
            return recheck_results[0]
            
        # Fallback: ambil yang paling lengkap (paling banyak field yang tidak null)
        def count_filled_fields(data):
            count = 0
            for key, value in data.items():
                if value is not None and value != "":
                    count += 1
            return count
            
        return max(data_list, key=count_filled_fields)

    def merge_bpkb_data(self, data_list: List[Dict]) -> Dict:
        """
        Merge data khusus untuk BPKB (jika ada multiple chunks)
        """
        if not data_list:
            return {}
        
        # BPKB format baru, ambil yang terbaik
        return self.get_best_simple_result(data_list)

    def merge_shm_data(self, data_list: List[Dict]) -> Dict:
        """
        Merge data khusus untuk SHM (format original tetap dipertahankan)
        """
        if not data_list:
            return {
                "tipe_dokumen": "Sertifikat Hak Milik",
                "status_kepatuhan_format": "Bad",
                "alasan_validasi": "",
                "analisa_kualitas_dokumen": "",
                "halaman_1": {
                    "jenis_hak": None,
                    "nomor_sertifikat": None,
                    "nib": None,
                    "kode_sertifikat": None,
                    "pemegang_hak": None,
                    "luas": None,
                    "kelurahan": None,
                    "kecamatan": None,
                    "kota": None
                },
                "halaman_2": {
                    "tanggal_penerbitan": None,
                    "luas_nib": None,
                    "letak_tanah": None
                },
                "surat_ukur": {
                    "nomor_surat_ukur": None,
                    "luas": None,
                    "kelurahan": None,
                    "kecamatan": None,
                    "kota": None
                }
            }
        
        # Untuk SHM, merge secara manual karena struktur nested
        return self.get_best_simple_result(data_list)

    def extract_json_only(self, text: str) -> Dict:
        """Ambil JSON murni dari text response, atau bungkus sebagai {'result': ...}."""
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
    Contoh pemakaian lokal dengan opsi tipe dokumen:
    
        python pdf_processor_optimized.py somefile.pdf --type sku --debug
        python pdf_processor_optimized.py somefile.pdf --type npwp --debug
        python pdf_processor_optimized.py somefile.pdf --type bpkb --debug
        python pdf_processor_optimized.py somefile.pdf --type shm --debug
        python pdf_processor_optimized.py somefile.pdf --type nib --debug
        python pdf_processor_optimized.py image.jpg --type ktp --debug
        python pdf_processor_optimized.py somefile.pdf --debug  # auto-detect
    """
    config = {
        "api_key": os.getenv("OPENWEBUI_API_KEY", "dummy-api-key"),
        "base_url": os.getenv("OPENWEBUI_BASE_URL", "http://localhost:8080"),
        "model": os.getenv("OPENWEBUI_MODEL", "image-screening-shmshm-elektronik"),
    }
    
    # Parse document type dari command line
    if "--type" in sys.argv:
        type_index = sys.argv.index("--type")
        if type_index + 1 < len(sys.argv):
            doc_type = sys.argv[type_index + 1]
            if doc_type in ["sku", "npwp", "bpkb", "shm", "nib", "ktp", "auto"]:
                config["document_type"] = doc_type
                print(f"Document type set to: {doc_type}", file=sys.stderr)

    processor = SilentPDFProcessor(config)

    if "--debug" in sys.argv:
        processor.enable_logging = True
        processor.log("OPTIMIZED Debug mode enabled")
    elif "--silent" in sys.argv:
        processor.enable_logging = False
    else:
        processor.enable_logging = True

    file_args = [arg for arg in sys.argv[1:] if not arg.startswith("--") and arg not in ["sku", "npwp", "bpkb", "shm", "nib", "ktp", "auto"]]
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

    common_paths = ["Dokumen/shmars.pdf", "Dokumen\\shmars.pdf", "shmars.pdf", "nib.pdf", "bpkb.pdf", "nib.jpg", "ktp.jpg", "npwp.pdf", "sku.pdf"]
    for path in common_paths:
        if os.path.exists(path):
            processor.log(f"Auto-detected file: {path}")
            result = processor.process_file(path)
            processor.enable_logging = False
            print(json.dumps(result, ensure_ascii=False))
            return

    if processor.enable_logging:
        processor.log("No files found")
    print(json.dumps({"error": "No files found"}))


if __name__ == "__main__":
    main()

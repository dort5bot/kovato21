# handler_loader_ultra_simple.py
"""
deepsek
02/12/25

ULTRA SIMPLE Handler Loader for Aiogram 3.x
- Tek seferlik yükleme için optimize edildi
- NO CACHE (gereksiz)
- NO COMPLEXITY (basit tut)
- Sadece işini yap: handler dosyalarını bul, import et, router'ı ekle
Aiogram 3.x handler’larını hızlı ve basit bir şekilde yüklemek için ideal bir otomatik handler loader.
# KULLANIM
    # EN BASİT KULLANIM
    loader = HandlerLoader(dp, "handlers")
    result = await loader.load_handlers()
    
    # VEYA tek satır
    result = await load_handlers(dp, "handlers")
"""

import importlib.util
import logging
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from aiogram import Dispatcher, Router

logger = logging.getLogger(__name__)

@dataclass
class LoadResult:
    """Loading result - sadece gerekli metrikler."""
    loaded: int = 0
    failed: int = 0
    skipped: int = 0
    scanned: int = 0
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for compatibility."""
        return {
            'loaded': self.loaded,
            'failed': self.failed,
            'skipped': self.skipped,
            'total_files': self.scanned,
            'errors': self.errors,
            'loaded_handlers': []  # Eski interface için boş liste
        }

class HandlerLoader:
    """
    TEK SEÇERLİK YÜKLEME için optimize edilmiş Handler Loader.
    Cache YOK, thread pool YOK, complexity YOK.
    """
    
    def __init__(
        self,
        dispatcher: Dispatcher,
        base_path: str = "handlers",
        handler_dirs: Optional[List[str]] = None
    ):
        self.dispatcher = dispatcher
        self.base_dir = Path(base_path).resolve()
        self.handler_dirs = handler_dirs or ["commands", "callbacks", "admin", "states"]
        
        logger.info(f"🔄 HandlerLoader başlatıldı: {self.base_dir}")
    
    async def load_handlers(self, dispatcher: Optional[Dispatcher] = None) -> Dict[str, Any]:
        """
        Handler'ları yükle - TEK SEÇERLİK.
        
        Args:
            dispatcher: Alternatif dispatcher (opsiyonel)
        
        Returns:
            Yükleme sonuçları
        """
        result = LoadResult()
        
        try:
            # Kullanılacak dispatcher'ı belirle
            target_dispatcher = dispatcher or self.dispatcher
            
            # 1. Dizin var mı kontrol et
            if not self.base_dir.exists():
                error_msg = f"Handler dizini bulunamadı: {self.base_dir}"
                logger.error(f"❌ {error_msg}")
                result.errors.append(error_msg)
                return result.to_dict()
            
            # 2. Tüm handler dosyalarını yükle
            await self._load_all_handlers(target_dispatcher, result)
            
            # 3. Sonuçları logla
            self._log_summary(result)
            
        except Exception as e:
            error_msg = f"Kritik yükleme hatası: {str(e)}"
            logger.error(f"💥 {error_msg}")
            result.errors.append(error_msg)
            result.failed += 1
        
        return result.to_dict()
    
    async def _load_all_handlers(self, dispatcher: Dispatcher, result: LoadResult):
        """Tüm handler dosyalarını yükle."""
        # Tüm tarama dizinleri
        scan_dirs = self._get_scan_directories()
        
        for dir_path in scan_dirs:
            await self._load_handlers_from_dir(dir_path, dispatcher, result)
    
    def _get_scan_directories(self) -> List[Path]:
        """Taranacak dizinleri listele."""
        directories = [self.base_dir]  # Kök dizin
        
        # Alt dizinleri ekle
        for subdir in self.handler_dirs:
            dir_path = self.base_dir / subdir
            if dir_path.exists():
                directories.append(dir_path)
        
        return directories
    
    async def _load_handlers_from_dir(self, dir_path: Path, dispatcher: Dispatcher, result: LoadResult):
        """Bir dizindeki handler dosyalarını yükle."""
        try:
            # .py dosyalarını bul
            py_files = list(dir_path.glob("*.py"))
            py_files = [f for f in py_files if not f.name.startswith("_")]
            
            if not py_files:
                return
            
            rel_path = dir_path.relative_to(self.base_dir)
            logger.info(f"📁 {rel_path}: {len(py_files)} dosya")
            
            # Dosyaları sırayla işle (basit tut)
            for file_path in py_files:
                result.scanned += 1
                await self._load_single_handler(file_path, dispatcher, result)
                
        except Exception as e:
            logger.error(f"📂 {dir_path.name} tarama hatası: {e}")
    
    async def _load_single_handler(self, file_path: Path, dispatcher: Dispatcher, result: LoadResult):
        """Tek bir handler dosyasını yükle."""
        try:
            # Modül adı oluştur (sadece logging için)
            module_name = self._generate_module_name(file_path)
            
            # 1. Modülü import et
            module = self._import_module_safely(file_path, module_name)
            if module is None:
                result.failed += 1
                return
            
            # 2. Router kontrolü
            if not hasattr(module, 'router'):
                logger.debug(f"⏭️ Router yok: {file_path.name}")
                result.skipped += 1
                return
            
            # 3. Router tip kontrolü
            router = module.router
            if not isinstance(router, Router):
                logger.error(f"❌ Geçersiz router tipi: {file_path.name}")
                result.failed += 1
                result.errors.append(f"Router instance değil: {file_path.name}")
                return
            
            # 4. Router'ı dispatcher'a ekle
            dispatcher.include_router(router)
            
            # 5. Başarılı
            result.loaded += 1
            logger.info(f"✅ {file_path.relative_to(self.base_dir)} yüklendi")
            
        except Exception as e:
            error_msg = f"{file_path.name} yükleme hatası: {str(e)}"
            logger.error(f"❌ {error_msg}")
            result.errors.append(error_msg)
            result.failed += 1
    
    def _import_module_safely(self, file_path: Path, module_name: str):
        """Güvenli modül import."""
        try:
            # Eski modülü temizle (güvenlik için)
            if module_name in sys.modules:
                del sys.modules[module_name]
            
            # Spec oluştur
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None or spec.loader is None:
                logger.error(f"⚠️ Spec oluşturulamadı: {file_path.name}")
                return None
            
            # Modülü oluştur ve yükle
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            return module
            
        except Exception as e:
            logger.error(f"⚠️ Import hatası {file_path.name}: {e}")
            return None
    
    def _generate_module_name(self, file_path: Path) -> str:
        """Basit modül adı oluştur."""
        try:
            rel_path = file_path.relative_to(self.base_dir)
            # Örnek: handlers/commands/start.py -> handlers_commands_start
            return f"handlers_{str(rel_path.with_suffix('')).replace('/', '_')}"
        except:
            return f"handlers_{file_path.stem}"
    
    def _log_summary(self, result: LoadResult):
        """Sonuçları özetle."""
        logger.info("=" * 30)
        logger.info("📊 HANDLER YÜKLEME SONUÇLARI")
        logger.info(f"Taranan dosya: {result.scanned}")
        logger.info(f"✅ Başarılı: {result.loaded}")
        logger.info(f"⏭️ Atlanan: {result.skipped}")
        logger.info(f"❌ Başarısız: {result.failed}")
        
        if result.errors:
            logger.warning(f"⚠️ Hatalar ({len(result.errors)}):")
            for i, error in enumerate(result.errors[:3], 1):
                logger.warning(f"  {i}. {error}")
            if len(result.errors) > 3:
                logger.warning(f"  ... ve {len(result.errors) - 3} hata daha")
        
        # Başarı oranı
        if result.scanned > 0:
            success_rate = (result.loaded / result.scanned) * 100
            logger.info(f"📈 Başarı oranı: {success_rate:.1f}%")
        
        logger.info("=" * 50)

# BASİT FONKSİYON (opsiyonel)
async def load_handlers(dispatcher: Dispatcher, base_path: str = "handlers") -> Dict[str, Any]:
    """Tek satırda handler yükleme."""
    loader = HandlerLoader(dispatcher, base_path)
    return await loader.load_handlers()



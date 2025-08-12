#!/usr/bin/env python3
"""Quick test to verify archive system works"""

import tempfile
import tarfile
from pathlib import Path
import sys

# Add the tellus package to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_imports():
    """Test that all our new classes can be imported"""
    print("Testing imports...")
    
    try:
        from tellus.simulation.simulation import (
            CacheManager, CacheConfig, TagSystem, PathMapper, PathMapping,
            ArchiveManifest, CompressedArchive, ArchiveRegistry,
            CLIProgressCallback
        )
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality"""
    print("\nTesting basic functionality...")
    
    try:
        from tellus.simulation.simulation import CacheManager, TagSystem
        
        # Test CacheManager
        cache_manager = CacheManager()
        stats = cache_manager.get_cache_stats()
        print(f"✓ CacheManager created, cache dir: {stats['cache_dir']}")
        
        # Test TagSystem
        tag_system = TagSystem()
        tags = tag_system.tag_file("input/data.nc")
        print(f"✓ TagSystem created, tagged 'input/data.nc' as: {tags}")
        
        return True
        
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("Quick Archive System Test")
    print("=" * 40)
    
    success = True
    success &= test_imports()
    success &= test_basic_functionality()
    
    if success:
        print("\n✓ All tests passed! Ready to run full demos.")
        return 0
    else:
        print("\n✗ Some tests failed.")
        return 1

if __name__ == "__main__":
    exit(main())
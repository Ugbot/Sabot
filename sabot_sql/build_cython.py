#!/usr/bin/env python3
"""
Build script for SabotSQL Cython bindings
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def build_cython_bindings():
    """Build SabotSQL Cython bindings"""
    print("🔧 Building SabotSQL Cython bindings...")
    
    # Change to sabot_sql directory
    sabot_sql_dir = Path(__file__).parent
    os.chdir(sabot_sql_dir)
    
    # Check if Cython is available
    try:
        import Cython
        print(f"✅ Cython version: {Cython.__version__}")
    except ImportError:
        print("❌ Cython not found. Installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "cython"], check=True)
    
    # Check if pyarrow is available
    try:
        import pyarrow as pa
        print(f"✅ PyArrow version: {pa.__version__}")
    except ImportError:
        print("❌ PyArrow not found. Installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "pyarrow"], check=True)
    
    # Build the extension
    print("🔨 Building Cython extension...")
    try:
        subprocess.run([sys.executable, "setup.py", "build_ext", "--inplace"], check=True)
        print("✅ Cython bindings built successfully!")
        
        # Check if the module was created
        if os.path.exists("sabot_sql.cpython-313-darwin.so") or os.path.exists("sabot_sql.so"):
            print("✅ SabotSQL module created")
        else:
            print("⚠️  SabotSQL module not found, but build completed")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Build failed: {e}")
        return False
    
    return True

def test_cython_bindings():
    """Test the Cython bindings"""
    print("\n🧪 Testing Cython bindings...")
    
    try:
        # Import the module
        import sabot_sql
        print("✅ SabotSQL module imported successfully")
        
        # Test basic functionality
        bridge = sabot_sql.create_sabot_sql_bridge()
        print("✅ SabotSQL bridge created")
        
        # Test extensions
        flink_ext = sabot_sql.create_flink_extension()
        print("✅ Flink extension created")
        
        questdb_ext = sabot_sql.create_questdb_extension()
        print("✅ QuestDB extension created")
        
        # Test agent execution
        result = sabot_sql.execute_sql_on_agent(bridge, "SELECT 1 as test", "test_agent")
        print("✅ Agent execution test passed")
        
        print("🎉 All Cython binding tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Cython binding test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main build function"""
    print("🚀 SabotSQL Cython Bindings Builder")
    print("=" * 40)
    
    # Build bindings
    if not build_cython_bindings():
        print("❌ Build failed")
        return 1
    
    # Test bindings
    if not test_cython_bindings():
        print("❌ Tests failed")
        return 1
    
    print("\n🎉 SabotSQL Cython bindings built and tested successfully!")
    print("Ready for orchestrator integration!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

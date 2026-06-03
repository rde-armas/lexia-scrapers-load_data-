import asyncio
import os
import sys
from pathlib import Path

# Add the project root to sys.path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from services.service import EmbeddingProvider

async def main():
    texts = ["El Tribunal de Apelaciones en lo Penal de 3er Turno confirmó la sentencia."]
    print(f"Testing dense vector output for texts: {texts}")
    
    try:
        results = await EmbeddingProvider.fetch_async(texts)
        for i, embedding in enumerate(results):
            print(f"\nResult {i}:")
            print(f"- Dense vector length: {len(embedding) if embedding else 'None'}")
            
            if embedding and len(embedding) > 0:
                print("✅ Success: Dense vector retrieved!")
            else:
                print("❌ Failure: No vector retrieved.")
                
    except Exception as e:
        print(f"❌ Error during test: {e}")

if __name__ == "__main__":
    asyncio.run(main())

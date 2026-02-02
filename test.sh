#!/bin/bash
# Fix the naming conflict between your pyspark folder and the PySpark package

echo "🔧 Fixing PySpark naming conflict..."

# Rename the pyspark folder to pipeline (to avoid conflict)
if [ -d "pyspark" ]; then
    echo "Renaming 'pyspark' folder to 'pipeline'..."
    mv pyspark pipeline
    echo "✅ Renamed pyspark → pipeline"
else
    echo "⚠️  'pyspark' folder not found"
fi

# Update run_pipeline.py imports
echo "Updating run_pipeline.py imports..."
sed -i.bak "s|sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'pyspark'))|sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'pipeline'))|g" run_pipeline.py

echo ""
echo "✅ Fix complete!"
echo ""
echo "Now run:"
echo "  python run_pipeline.py"
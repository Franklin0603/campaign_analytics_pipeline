#!/bin/bash

echo "📁 Organizing documentation..."

# Create directory structure
mkdir -p docs/architecture
mkdir -p docs/setup
mkdir -p docs/images
mkdir -p dbt_project/docs

echo "✅ Directory structure created"

# Move or create files appropriately
# (diagrams.md already created above)

echo "📝 Documentation organized!"
echo ""
echo "Final structure:"
echo "campaign_analytics_pipeline/"
echo "├── README.md                    ← Main project overview"
echo "├── docs/"
echo "│   ├── architecture/"
echo "│   │   └── diagrams.md          ← All Mermaid diagrams"
echo "│   ├── setup/"
echo "│   │   └── docker-setup.md      ← Setup guides"
echo "│   └── images/                  ← Screenshots"
echo "└── dbt_project/"
echo "    ├── models/"
echo "    │   ├── docs.md              ← dbt model docs"
echo "    │   └── */_*__models.yml     ← YAML docs"
echo "    └── docs/                    ← Optional dbt-specific docs"

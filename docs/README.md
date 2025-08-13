# DayDreaming Documentation

This directory contains all documentation for the DayDreaming LLM experiment project, organized into logical sections for easy navigation.

## Directory Structure

### 📐 `architecture/` - System Architecture & Design
Contains high-level system design documents, architectural decisions, and technical implementation details:

- **`architecture.md`** - Main pipeline architecture overview with Dagster implementation details, evaluation asset patterns, and search strategy
- **`tiered_llm_concurrency.md`** - Concurrency control and resource management design
- **`new_processing_pipeline.md`** - Design for the new processing pipeline with generation response parsing

### 📚 `guides/` - User Guides & How-To
Contains practical guides for setting up, running, and troubleshooting the system:

- **`operating_guide.md`** - Comprehensive guide covering setup, execution, and troubleshooting

## Quick Start

1. **New to the project?** Start with `architecture/architecture.md` for the big picture
2. **Setting up or running experiments?** Check `guides/operating_guide.md`
3. **Understanding the code?** Read `architecture/architecture.md` for Dagster implementation details

## Contributing

When adding new documentation:
- **Architecture/Design decisions** → `architecture/`
- **User guides, setup instructions, troubleshooting** → `guides/`
- **API documentation** → `architecture/` (when we add it later)

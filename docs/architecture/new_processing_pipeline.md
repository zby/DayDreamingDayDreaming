# New Processing Pipeline with Generation Response Parsing

## Overview
The new pipeline introduces a parsing step between generation and evaluation to extract essay portions from LLM responses while preserving the full context for better generation quality.

## Pipeline Flow

```mermaid
graph TD
    A[Raw Data Assets] --> B[Content Combinations]
    B --> C[Generation Tasks]
    C --> D[Generation Prompts]
    D --> E[Generation Responses]
    E --> F[Parsed Generation Responses]
    F --> G[Evaluation Prompts]
    G --> H[Evaluation Responses]
    H --> I[Parsed Scores]
    I --> J[Results Analysis]
    J --> K[Final Results]
    
    style F [fill:#e1f5fe,stroke:#01579b,stroke-width:2px]
    style G [fill:#f3e5f5,stroke:#4a148c,stroke-width:2px]
```

## Key Changes

### 1. New Asset: `parsed_generation_responses`
- **Location**: `daydreaming_dagster/assets/results_processing.py`
- **Partitioning**: Uses `generation_tasks_partitions` (same as generation responses)
- **Purpose**: Extracts essay portions from full generation responses
- **Output**: Structured data with essay content, parsing metadata, and validation results

### 2. Updated Evaluation Pipeline
- **Input Change**: `evaluation_prompt` now depends on `parsed_generation_responses` instead of `generation_response`
- **Content Source**: Uses extracted essay content instead of full response
- **Fallback Support**: Maintains backward compatibility during transition

### 3. Parsing Strategies
- **XML Tags**: For newer templates (`creative-synthesis-v4`, `creative-synthesis-v5`)
- **Section Headers**: For templates with clear section divisions
- **Fallback**: Treats entire response as essay for older templates

## Data Flow Details

### Generation → Parsing
```
generation_response (full response with thinking + essay + endnotes)
    ↓
parsed_generation_responses (extracted essay + metadata)
    ↓
evaluation_prompt (essay content only)
```

### Parsing Asset Output Structure
```python
{
    "essay_content": "Extracted essay text...",
    "parsing_metadata": {
        "strategy": "xml_tags",
        "template_id": "creative-synthesis-v4",
        "model_name": "gemini-2.5-flash",
        "parsing_success": True,
        "sections_found": ["thinking", "essay", "endnotes"]
    },
    "validation_results": {
        "is_valid": True,
        "metrics": {"word_count": 1500, "paragraph_count": 8}
    },
    "full_response": "Original complete response..."
}
```

## Benefits

### 1. **Evaluation Quality**
- Evaluators focus on essay content only
- Consistent content length and structure
- Reduced noise from thinking/analysis sections

### 2. **Generation Quality**
- LLMs can still use full context for better thinking
- Structured prompts encourage better organization
- Maintains the benefits of multi-step reasoning

### 3. **Pipeline Flexibility**
- Easy to switch parsing strategies
- Template-aware parsing
- Fallback mechanisms for robustness

## Implementation Details

### Asset Dependencies
```python
# New dependency chain
generation_response → parsed_generation_responses → evaluation_prompt
```

### Resource Requirements
- `parsing_results_io_manager`: For storing parsing results
- `generation_response_io_manager`: For reading generation responses

### Partitioning Strategy
- Parsing asset uses `generation_tasks_partitions`
- Maintains 1:1 relationship with generation responses
- Enables parallel processing of parsing tasks

## Migration Strategy

### Phase 1: Deploy New Assets
1. Deploy `parsed_generation_responses` asset
2. Materialize for existing generation responses
3. Verify parsing quality

### Phase 2: Update Evaluation Pipeline
1. Update `evaluation_prompt` dependencies
2. Test with parsed responses
3. Monitor evaluation quality

### Phase 3: Cleanup
1. Remove old direct dependencies
2. Optimize parsing strategies
3. Add monitoring and alerting

## Testing Strategy

### Unit Tests
- `test_parsed_generation_responses.py`: Asset functionality
- `test_generation_response_parser.py`: Parser utilities

### Integration Tests
- End-to-end pipeline validation
- Parsing strategy selection
- Fallback mechanism testing

### Performance Tests
- Parsing throughput
- Memory usage
- Error handling under load

## Monitoring and Observability

### Key Metrics
- Parsing success rate by template/model
- Essay extraction accuracy
- Processing time per response
- Fallback usage frequency

### Error Tracking
- Parsing failures by strategy
- Template/model compatibility issues
- Validation failures

### Quality Metrics
- Essay content length distribution
- Content structure consistency
- Template adherence rates

## Future Enhancements

### 1. **Advanced Parsing**
- Machine learning-based content extraction
- Template-specific parsing rules
- Multi-language support

### 2. **Content Validation**
- Plagiarism detection
- Content quality scoring
- Style consistency checks

### 3. **Pipeline Optimization**
- Caching parsed results
- Incremental parsing updates
- Parallel processing optimization

## Risk Mitigation

### 1. **Data Loss Prevention**
- Fallback to original responses
- Comprehensive error logging
- Data validation checks

### 2. **Performance Impact**
- Asynchronous parsing
- Resource usage monitoring
- Graceful degradation

### 3. **Quality Assurance**
- A/B testing of parsing strategies
- Human review of edge cases
- Continuous validation metrics

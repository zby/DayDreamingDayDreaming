#!/usr/bin/env python3
"""
Script to generate all prompts for a given concept level and number of concepts (k).

Usage:
    python generate_prompts.py --level paragraph --k 2
    python generate_prompts.py --level sentence --k 3 --manifest data/concepts/day_dreaming_concepts.json
"""

import argparse
from pathlib import Path
from daydreaming_experiment.concept_db import ConceptDB
from daydreaming_experiment.prompt_factory import PromptFactory, PromptIterator


def main():
    parser = argparse.ArgumentParser(description="Generate all prompts for given level and k")
    parser.add_argument("--level", default="paragraph", choices=["sentence", "paragraph", "article"],
                       help="Concept granularity level (default: paragraph)")
    parser.add_argument("--k", type=int, default=3,
                       help="Number of concepts per combination (default: 3)")
    parser.add_argument("--manifest", default="data/concepts/day_dreaming_concepts.json",
                       help="Path to concept manifest file")
    parser.add_argument("--output", help="Output file (default: stdout)")
    parser.add_argument("--strict", action="store_true",
                       help="Require exact level match, fail if requested level is unavailable (default: non-strict with fallback)")
    
    args = parser.parse_args()
    
    # Load concepts
    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"Error: Manifest file not found: {args.manifest}")
        return 1
    
    concept_db = ConceptDB.load(str(manifest_path))
    print(f"Loaded {len(concept_db)} concepts from {args.manifest}")
    
    # Generate combinations
    combinations = list(concept_db.get_combinations(args.k))
    if not combinations:
        print(f"Error: No {args.k}-combinations possible with {len(concept_db)} concepts")
        return 1
    
    print(f"Generating prompts for {len(combinations)} combinations at '{args.level}' level")
    
    # Create prompt iterator
    factory = PromptFactory()
    strict = args.strict  # Use strict flag directly (default is False = non-strict)
    iterator = PromptIterator(factory, combinations, args.level, strict=strict)
    
    total_prompts = iterator.get_total_count()
    print(f"Total prompts to generate: {total_prompts}")
    print("=" * 80)
    
    # Generate all prompts
    all_prompts = iterator.generate_all()
    
    # Output prompts
    output_lines = []
    for i, (concepts, template_idx, prompt) in enumerate(all_prompts, 1):
        concept_names = [c.name for c in concepts]
        
        header = f"PROMPT {i}/{total_prompts}"
        header += f" | Template {template_idx} | Concepts: {', '.join(concept_names)}"
        
        output_lines.append(header)
        output_lines.append("=" * len(header))
        output_lines.append(prompt)
        output_lines.append("")  # Blank line separator
    
    output_text = "\n".join(output_lines)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(output_text)
        print(f"Prompts written to {args.output}")
    else:
        print(output_text)
    
    return 0


if __name__ == "__main__":
    exit(main())
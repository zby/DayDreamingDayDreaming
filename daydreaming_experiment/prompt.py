"""
Prompt generation module that uses DAG structure to create templated prompts.
"""
from typing import Dict, List, Optional, Any
import string
from dataclasses import dataclass

from .concept_dag import ConceptDAG, ConceptLevel


@dataclass
class PromptTemplate:
    """Represents a prompt template with placeholders for concepts."""
    template: str
    required_concepts: List[str]
    concept_level: ConceptLevel = ConceptLevel.PARAGRAPH


class PromptGenerator:
    """Generates prompts using DAG concepts and templates."""
    
    def __init__(self, dag: ConceptDAG):
        self.dag = dag
        
    def extract_concepts_from_node(self, node_id: str, level: ConceptLevel = ConceptLevel.PARAGRAPH) -> Dict[str, str]:
        """
        Extract concepts from a DAG node and its ancestors at specified detail level.
        
        Args:
            node_id: ID of the node to extract concepts from
            level: Level of detail for concept extraction
            
        Returns:
            Dictionary mapping concept names to their content at specified level
        """
        if node_id not in self.dag.nodes:
            raise ValueError(f"Node '{node_id}' does not exist")
            
        concepts = {}
        visited = set()
        
        def collect_concepts(current_id: str) -> None:
            if current_id in visited:
                return
            visited.add(current_id)
            
            node = self.dag.nodes[current_id]
            node_content = self.dag.get_node_content(current_id)
            concepts[current_id] = self._format_concept_by_level(node_content, level)
            
            # Collect from parents
            for parent_id in node.parents:
                collect_concepts(parent_id)
                
        collect_concepts(node_id)
        return concepts
        
    def _format_concept_by_level(self, content: str, level: ConceptLevel) -> str:
        """
        Format concept content based on the specified level.
        
        Args:
            content: Original concept content
            level: Desired level of detail
            
        Returns:
            Formatted content at specified level
        """
        if level == ConceptLevel.ARTICLE:
            # Return full content as article-length
            return content
        elif level == ConceptLevel.PARAGRAPH:
            # Extract or truncate to paragraph length (roughly first 200-500 chars)
            sentences = content.split('. ')
            if len(sentences) <= 3:
                return content
            return '. '.join(sentences[:3]) + '.'
        elif level == ConceptLevel.SENTENCE:
            # Extract or truncate to single sentence
            first_sentence = content.split('.')[0].strip()
            return first_sentence + '.' if first_sentence else content[:100] + '...'
        else:
            return content
        
    def generate_prompt(self, template: PromptTemplate, node_id: str, 
                       additional_context: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate a prompt using a template and concepts from a DAG node.
        
        Args:
            template: The prompt template to use
            node_id: ID of the DAG node to extract concepts from
            additional_context: Optional additional context variables
            
        Returns:
            Generated prompt string
        """
        concepts = self.extract_concepts_from_node(node_id, template.concept_level)
        
        # Combine with additional context if provided
        context = dict(concepts)
        if additional_context:
            context.update(additional_context)
            
        # Check if all required concepts are available
        missing_concepts = []
        for required in template.required_concepts:
            if required not in context:
                missing_concepts.append(required)
                
        if missing_concepts:
            raise ValueError(f"Missing required concepts: {missing_concepts}")
            
        # Use string.Template for safe substitution
        template_obj = string.Template(template.template)
        
        try:
            return template_obj.substitute(context)
        except KeyError as e:
            raise ValueError(f"Template contains undefined placeholder: {e}")
            
    def generate_prompts_for_leaf_nodes(self, template: PromptTemplate,
                                      additional_context: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """
        Generate prompts for all leaf nodes in the DAG.
        
        Args:
            template: The prompt template to use
            additional_context: Optional additional context variables
            
        Returns:
            Dictionary mapping node IDs to generated prompts
        """
        leaf_nodes = self.dag.get_leaf_nodes()
        prompts = {}
        
        for node_id in leaf_nodes:
            try:
                prompts[node_id] = self.generate_prompt(template, node_id, additional_context)
            except ValueError as e:
                # Skip nodes that don't have required concepts
                prompts[node_id] = f"ERROR: {str(e)}"
                
        return prompts
        
    def get_available_concepts(self, node_id: str) -> List[str]:
        """
        Get list of available concept names for a given node.
        
        Args:
            node_id: ID of the node to check
            
        Returns:
            List of available concept names
        """
        concepts = self.extract_concepts_from_node(node_id)
        return list(concepts.keys())
        
    def generate_multi_level_prompts(self, base_template: str, node_id: str,
                                   additional_context: Optional[Dict[str, Any]] = None) -> Dict[ConceptLevel, str]:
        """
        Generate prompts at all three concept levels for comparison.
        
        Args:
            base_template: Template string with concept placeholders
            node_id: ID of the DAG node to extract concepts from
            additional_context: Optional additional context variables
            
        Returns:
            Dictionary mapping concept levels to generated prompts
        """
        results = {}
        
        for level in ConceptLevel:
            template = PromptTemplate(
                template=base_template,
                required_concepts=[],  # Will be inferred from template
                concept_level=level
            )
            
            try:
                results[level] = self.generate_prompt(template, node_id, additional_context)
            except ValueError as e:
                results[level] = f"ERROR: {str(e)}"
                
        return results


class PromptTemplateLibrary:
    """Library of common prompt templates."""
    
    @staticmethod
    def concept_exploration_template(level: ConceptLevel = ConceptLevel.PARAGRAPH) -> PromptTemplate:
        """Template for exploring concepts at different detail levels."""
        return PromptTemplate(
            template="Analyze this concept: $concept\n\nProvide insights about its implications and connections.",
            required_concepts=["concept"],
            concept_level=level
        )
        
    @staticmethod
    def hierarchical_concept_template(level: ConceptLevel = ConceptLevel.PARAGRAPH) -> PromptTemplate:
        """Template that uses hierarchical DAG structure."""
        return PromptTemplate(
            template="Foundation: $foundation\n\nBuilding on this: $development\n\nNow consider: $exploration",
            required_concepts=["foundation", "development", "exploration"],
            concept_level=level
        )
        
    @staticmethod
    def comparative_analysis_template(level: ConceptLevel = ConceptLevel.SENTENCE) -> PromptTemplate:
        """Template for comparing multiple concepts."""
        return PromptTemplate(
            template="Compare and contrast:\n1. $concept1\n2. $concept2\n\nWhat relationships emerge?",
            required_concepts=["concept1", "concept2"],
            concept_level=level
        )
        
    @staticmethod
    def synthesis_template(level: ConceptLevel = ConceptLevel.PARAGRAPH) -> PromptTemplate:
        """Template for synthesizing multiple concepts."""
        return PromptTemplate(
            template="Synthesize these ideas:\n\n$idea1\n\n$idea2\n\nWhat new insights emerge?",
            required_concepts=["idea1", "idea2"],
            concept_level=level
        )
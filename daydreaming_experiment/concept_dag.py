"""
ConceptDAG structure implementation for the daydreaming experiment.
Supports three levels of concept representation: sentence, paragraph, and article.
"""
from typing import Dict, List, Set, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import os
from pathlib import Path


class ConceptLevel(Enum):
    """Enumeration of concept representation levels."""
    SENTENCE = "sentence"
    PARAGRAPH = "paragraph"
    ARTICLE = "article"


@dataclass
class ConceptNode:
    """Represents a node in the concept DAG."""
    id: str
    content: Union[str, Path]  # String for sentence/paragraph, Path for article
    level: ConceptLevel
    parents: List[str] = field(default_factory=list)
    metadata: Dict = field(default_factory=dict)


class ConceptDAG:
    """Directed Acyclic Graph for managing concept nodes and their relationships."""
    
    def __init__(self, storage_dir: Optional[str] = None):
        """
        Initialize the ConceptDAG.
        
        Args:
            storage_dir: Directory for storing article-level concepts as files
        """
        self.nodes: Dict[str, ConceptNode] = {}
        self.storage_dir = Path(storage_dir) if storage_dir else Path("concept_articles")
        self.storage_dir.mkdir(exist_ok=True)
        
    def add_node(self, node_id: str, content: str, level: ConceptLevel, 
                 parents: Optional[List[str]] = None, metadata: Optional[Dict] = None) -> None:
        """
        Add a node to the DAG.
        
        Args:
            node_id: Unique identifier for the node
            content: Content for the node
            level: ConceptLevel (sentence, paragraph, or article)
            parents: List of parent node IDs
            metadata: Additional metadata for the node
        """
        if parents is None:
            parents = []
        if metadata is None:
            metadata = {}
            
        # Validate that all parents exist
        for parent in parents:
            if parent not in self.nodes:
                raise ValueError(f"Parent node '{parent}' does not exist")
        
        # Handle article-level content storage
        if level == ConceptLevel.ARTICLE:
            article_path = self.storage_dir / f"{node_id}.txt"
            with open(article_path, 'w', encoding='utf-8') as f:
                f.write(content)
            stored_content = article_path
        else:
            stored_content = content
            
        self.nodes[node_id] = ConceptNode(
            id=node_id,
            content=stored_content,
            level=level,
            parents=parents,
            metadata=metadata
        )
        
    def get_node_content(self, node_id: str) -> str:
        """
        Get the actual content of a node, reading from file if necessary.
        
        Args:
            node_id: ID of the node
            
        Returns:
            Content of the node
        """
        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist")
            
        node = self.nodes[node_id]
        
        if node.level == ConceptLevel.ARTICLE:
            # Read from file
            with open(node.content, 'r', encoding='utf-8') as f:
                return f.read()
        else:
            # Return string content directly
            return node.content
        
    def get_full_concept(self, node_id: str) -> str:
        """
        Construct the full concept by combining content from the node and all its ancestors.
        
        Args:
            node_id: ID of the node to construct concept for
            
        Returns:
            Full concept text
        """
        if node_id not in self.nodes:
            raise ValueError(f"Node '{node_id}' does not exist")
            
        visited = set()
        parts = []
        
        def traverse(node_id: str) -> None:
            if node_id in visited:
                return
            visited.add(node_id)
            
            content = self.get_node_content(node_id)
            parts.append(content)
            
            node = self.nodes[node_id]
            for parent_id in node.parents:
                traverse(parent_id)
                
        traverse(node_id)
        return "\n\n".join(reversed(parts))  # Root content should come first
        
    def get_leaf_nodes(self) -> Set[str]:
        """
        Identify all leaf nodes (nodes with no children).
        
        Returns:
            Set of leaf node IDs
        """
        # Find all nodes that are not parents of any other node
        all_nodes = set(self.nodes.keys())
        parent_nodes = set()
        
        for node in self.nodes.values():
            parent_nodes.update(node.parents)
            
        return all_nodes - parent_nodes
        
    def get_nodes_by_level(self, level: ConceptLevel) -> Dict[str, ConceptNode]:
        """
        Get all nodes of a specific concept level.
        
        Args:
            level: ConceptLevel to filter by
            
        Returns:
            Dictionary of node_id -> ConceptNode for the specified level
        """
        return {node_id: node for node_id, node in self.nodes.items() 
                if node.level == level}
        
    def validate_dag(self) -> bool:
        """
        Validate that the structure is a DAG (no cycles).
        
        Returns:
            True if valid DAG, False otherwise
        """
        visited = set()
        visiting = set()
        
        def has_cycle(node_id: str) -> bool:
            if node_id in visiting:
                return True
            if node_id in visited:
                return False
                
            visiting.add(node_id)
            node = self.nodes[node_id]
            
            for parent_id in node.parents:
                if has_cycle(parent_id):
                    return True
                    
            visiting.remove(node_id)
            visited.add(node_id)
            return False
            
        for node_id in self.nodes:
            if has_cycle(node_id):
                return False
                
        return True
        
    def save_to_disk(self, filepath: str) -> None:
        """
        Save the ConceptDAG structure to disk (excluding article content which is already stored).
        
        Args:
            filepath: Path to save the DAG structure
        """
        dag_data = {
            'storage_dir': str(self.storage_dir),
            'nodes': {}
        }
        
        for node_id, node in self.nodes.items():
            dag_data['nodes'][node_id] = {
                'id': node.id,
                'content': str(node.content) if node.level == ConceptLevel.ARTICLE else node.content,
                'level': node.level.value,
                'parents': node.parents,
                'metadata': node.metadata
            }
            
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(dag_data, f, indent=2, ensure_ascii=False)
            
    @classmethod
    def load_from_disk(cls, filepath: str) -> 'ConceptDAG':
        """
        Load a ConceptDAG structure from disk.
        
        Args:
            filepath: Path to the saved DAG structure
            
        Returns:
            ConceptDAG instance
        """
        with open(filepath, 'r', encoding='utf-8') as f:
            dag_data = json.load(f)
            
        dag = cls(storage_dir=dag_data['storage_dir'])
        
        for node_id, node_data in dag_data['nodes'].items():
            level = ConceptLevel(node_data['level'])
            
            if level == ConceptLevel.ARTICLE:
                content_path = Path(node_data['content'])
            else:
                content_path = node_data['content']
                
            # Recreate the node
            dag.nodes[node_id] = ConceptNode(
                id=node_data['id'],
                content=content_path,
                level=level,
                parents=node_data['parents'],
                metadata=node_data['metadata']
            )
            
        return dag
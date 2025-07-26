"""
Unit tests for ConceptDAG implementation.
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from daydreaming_experiment.concept_dag import ConceptDAG, ConceptLevel, ConceptNode


class TestConceptDAG:
    """Test cases for ConceptDAG class."""
    
    def setup_method(self):
        """Set up test fixtures with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.dag = ConceptDAG(storage_dir=self.temp_dir)
        
    def teardown_method(self):
        """Clean up temporary directory."""
        shutil.rmtree(self.temp_dir)
        
    def test_dag_creation(self):
        """Test creating a basic ConceptDAG."""
        assert isinstance(self.dag, ConceptDAG)
        assert len(self.dag.nodes) == 0
        assert self.dag.storage_dir.exists()
        
    def test_add_sentence_node(self):
        """Test adding sentence-level nodes."""
        self.dag.add_node("sentence1", "This is a sentence.", ConceptLevel.SENTENCE)
        
        assert "sentence1" in self.dag.nodes
        node = self.dag.nodes["sentence1"]
        assert node.content == "This is a sentence."
        assert node.level == ConceptLevel.SENTENCE
        assert node.parents == []
        
    def test_add_paragraph_node(self):
        """Test adding paragraph-level nodes."""
        self.dag.add_node("para1", "This is a paragraph with multiple sentences. It contains more detail.", ConceptLevel.PARAGRAPH)
        
        assert "para1" in self.dag.nodes
        node = self.dag.nodes["para1"]
        assert node.level == ConceptLevel.PARAGRAPH
        
    def test_add_article_node(self):
        """Test adding article-level nodes (stored as files)."""
        article_content = "This is a full article with multiple paragraphs.\n\nIt contains comprehensive information about a topic."
        self.dag.add_node("article1", article_content, ConceptLevel.ARTICLE)
        
        assert "article1" in self.dag.nodes
        node = self.dag.nodes["article1"]
        assert node.level == ConceptLevel.ARTICLE
        assert isinstance(node.content, Path)
        assert node.content.exists()
        
        # Verify content can be read back
        retrieved_content = self.dag.get_node_content("article1")
        assert retrieved_content == article_content
        
    def test_add_node_with_parents(self):
        """Test adding nodes with parent relationships."""
        self.dag.add_node("root", "Root concept", ConceptLevel.SENTENCE)
        self.dag.add_node("child", "Child concept", ConceptLevel.SENTENCE, parents=["root"])
        
        assert "child" in self.dag.nodes
        child_node = self.dag.nodes["child"]
        assert child_node.parents == ["root"]
        
    def test_add_node_invalid_parent(self):
        """Test that adding node with non-existent parent raises error."""
        with pytest.raises(ValueError, match="Parent node 'nonexistent' does not exist"):
            self.dag.add_node("child", "Child content", ConceptLevel.SENTENCE, parents=["nonexistent"])
            
    def test_get_node_content_sentence(self):
        """Test getting content for sentence-level nodes."""
        self.dag.add_node("sentence1", "Test sentence.", ConceptLevel.SENTENCE)
        content = self.dag.get_node_content("sentence1")
        assert content == "Test sentence."
        
    def test_get_node_content_article(self):
        """Test getting content for article-level nodes."""
        article_content = "Full article content here."
        self.dag.add_node("article1", article_content, ConceptLevel.ARTICLE)
        content = self.dag.get_node_content("article1")
        assert content == article_content
        
    def test_get_node_content_nonexistent(self):
        """Test getting content for non-existent node raises error."""
        with pytest.raises(ValueError, match="Node 'nonexistent' does not exist"):
            self.dag.get_node_content("nonexistent")
            
    def test_get_full_concept(self):
        """Test constructing full concepts from node hierarchy."""
        self.dag.add_node("root", "Root concept", ConceptLevel.SENTENCE)
        self.dag.add_node("child", "Child concept", ConceptLevel.SENTENCE, parents=["root"])
        
        full_concept = self.dag.get_full_concept("child")
        assert full_concept == "Root concept\n\nChild concept"
        
    def test_get_leaf_nodes(self):
        """Test identifying leaf nodes."""
        self.dag.add_node("root", "Root", ConceptLevel.SENTENCE)
        self.dag.add_node("child1", "Child 1", ConceptLevel.SENTENCE, parents=["root"])
        self.dag.add_node("child2", "Child 2", ConceptLevel.SENTENCE, parents=["root"])
        self.dag.add_node("grandchild", "Grandchild", ConceptLevel.SENTENCE, parents=["child1"])
        
        leaf_nodes = self.dag.get_leaf_nodes()
        assert leaf_nodes == {"child2", "grandchild"}
        
    def test_get_nodes_by_level(self):
        """Test filtering nodes by concept level."""
        self.dag.add_node("sentence1", "Sentence", ConceptLevel.SENTENCE)
        self.dag.add_node("para1", "Paragraph", ConceptLevel.PARAGRAPH)
        self.dag.add_node("article1", "Article content", ConceptLevel.ARTICLE)
        
        sentence_nodes = self.dag.get_nodes_by_level(ConceptLevel.SENTENCE)
        paragraph_nodes = self.dag.get_nodes_by_level(ConceptLevel.PARAGRAPH)
        article_nodes = self.dag.get_nodes_by_level(ConceptLevel.ARTICLE)
        
        assert len(sentence_nodes) == 1
        assert "sentence1" in sentence_nodes
        assert len(paragraph_nodes) == 1
        assert "para1" in paragraph_nodes
        assert len(article_nodes) == 1
        assert "article1" in article_nodes
        
    def test_validate_dag(self):
        """Test DAG validation."""
        self.dag.add_node("root", "Root", ConceptLevel.SENTENCE)
        self.dag.add_node("child", "Child", ConceptLevel.SENTENCE, parents=["root"])
        assert self.dag.validate_dag() == True
        
    def test_save_and_load_dag(self):
        """Test saving and loading ConceptDAG to/from disk."""
        # Set up DAG with mixed content types
        self.dag.add_node("sentence1", "Test sentence", ConceptLevel.SENTENCE)
        self.dag.add_node("para1", "Test paragraph content", ConceptLevel.PARAGRAPH, parents=["sentence1"])
        self.dag.add_node("article1", "Full article content with details", ConceptLevel.ARTICLE, parents=["para1"])
        
        # Save to disk
        save_path = Path(self.temp_dir) / "test_dag.json"
        self.dag.save_to_disk(str(save_path))
        assert save_path.exists()
        
        # Load from disk
        loaded_dag = ConceptDAG.load_from_disk(str(save_path))
        
        # Verify structure is preserved
        assert len(loaded_dag.nodes) == 3
        assert "sentence1" in loaded_dag.nodes
        assert "para1" in loaded_dag.nodes
        assert "article1" in loaded_dag.nodes
        
        # Verify content is accessible
        assert loaded_dag.get_node_content("sentence1") == "Test sentence"
        assert loaded_dag.get_node_content("para1") == "Test paragraph content"
        assert loaded_dag.get_node_content("article1") == "Full article content with details"
        
        # Verify relationships are preserved
        assert loaded_dag.nodes["para1"].parents == ["sentence1"]
        assert loaded_dag.nodes["article1"].parents == ["para1"]
        
    def test_add_node_with_metadata(self):
        """Test adding nodes with metadata."""
        metadata = {"author": "test", "created": "2025-01-01"}
        self.dag.add_node("test_node", "Test content", ConceptLevel.SENTENCE, metadata=metadata)
        
        node = self.dag.nodes["test_node"]
        assert node.metadata == metadata


class TestConceptNode:
    """Test cases for ConceptNode dataclass."""
    
    def test_concept_node_creation(self):
        """Test creating ConceptNode instances."""
        node = ConceptNode(
            id="test_id",
            content="test content",
            level=ConceptLevel.SENTENCE,
            parents=["parent1"],
            metadata={"key": "value"}
        )
        
        assert node.id == "test_id"
        assert node.content == "test content"
        assert node.level == ConceptLevel.SENTENCE
        assert node.parents == ["parent1"]
        assert node.metadata == {"key": "value"}
        
    def test_concept_node_defaults(self):
        """Test ConceptNode with default values."""
        node = ConceptNode(
            id="test_id",
            content="test content",
            level=ConceptLevel.PARAGRAPH
        )
        
        assert node.parents == []
        assert node.metadata == {}


class TestConceptFormatting:
    """Test cases for concept formatting at different levels."""
    
    def setup_method(self):
        """Set up test fixtures."""
        from .prompt import PromptGenerator
        self.dag = ConceptDAG()
        self.generator = PromptGenerator(self.dag)
        
    def test_format_concept_article_level(self):
        """Test formatting concept at article level."""
        content = "This is a long article. It has multiple sentences. Each sentence adds detail."
        
        result = self.generator._format_concept_by_level(content, ConceptLevel.ARTICLE)
        
        assert result == content  # Should return unchanged
        
    def test_format_concept_paragraph_level(self):
        """Test formatting concept at paragraph level."""
        content = "First sentence. Second sentence. Third sentence. Fourth sentence. Fifth sentence."
        
        result = self.generator._format_concept_by_level(content, ConceptLevel.PARAGRAPH)
        
        assert result == "First sentence. Second sentence. Third sentence."
        
    def test_format_concept_sentence_level(self):
        """Test formatting concept at sentence level."""
        content = "First sentence. Second sentence. Third sentence."
        
        result = self.generator._format_concept_by_level(content, ConceptLevel.SENTENCE)
        
        assert result == "First sentence."
        
    def test_format_short_content(self):
        """Test formatting already short content."""
        content = "Short content."
        
        # Should remain unchanged at all levels
        article = self.generator._format_concept_by_level(content, ConceptLevel.ARTICLE)
        paragraph = self.generator._format_concept_by_level(content, ConceptLevel.PARAGRAPH)
        sentence = self.generator._format_concept_by_level(content, ConceptLevel.SENTENCE)
        
        assert article == content
        assert paragraph == content
        assert sentence == content


class TestConceptExtraction:
    """Test cases for extracting concepts from DAG at different levels."""
    
    def setup_method(self):
        """Set up test fixtures."""
        from .prompt import PromptGenerator
        self.dag = ConceptDAG()
        self.dag.add_node("root", "This is a comprehensive article about machine learning. Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models. It enables computer systems to improve their performance on a specific task through experience without being explicitly programmed. The field has grown rapidly in recent decades. Applications include computer vision, natural language processing, and robotics.", ConceptLevel.ARTICLE)
        self.dag.add_node("child1", "Neural networks are a key component of deep learning. They consist of interconnected nodes that process information.", ConceptLevel.PARAGRAPH, ["root"])
        self.dag.add_node("child2", "Supervised learning uses labeled training data. It includes classification and regression tasks.", ConceptLevel.PARAGRAPH, ["root"])
        self.dag.add_node("grandchild", "Convolutional neural networks excel at image recognition tasks.", ConceptLevel.SENTENCE, ["child1"])
        
        self.generator = PromptGenerator(self.dag)
        
    def test_extract_concepts_article_level(self):
        """Test extracting concepts at article level."""
        concepts = self.generator.extract_concepts_from_node("grandchild", ConceptLevel.ARTICLE)
        
        assert "grandchild" in concepts
        assert "child1" in concepts
        assert "root" in concepts
        assert len(concepts["root"]) > 100  # Should be full article length
        
    def test_extract_concepts_paragraph_level(self):
        """Test extracting concepts at paragraph level."""
        concepts = self.generator.extract_concepts_from_node("grandchild", ConceptLevel.PARAGRAPH)
        
        assert "grandchild" in concepts
        assert "child1" in concepts
        assert "root" in concepts
        # Should be shorter than article but longer than sentence (first 3 sentences)
        expected_paragraph = "This is a comprehensive article about machine learning. Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models. It enables computer systems to improve their performance on a specific task through experience without being explicitly programmed."
        assert concepts["root"] == expected_paragraph
        
    def test_extract_concepts_sentence_level(self):
        """Test extracting concepts at sentence level."""
        concepts = self.generator.extract_concepts_from_node("grandchild", ConceptLevel.SENTENCE)
        
        assert "grandchild" in concepts
        assert "child1" in concepts
        assert "root" in concepts
        # Should be just the first sentence
        expected_sentence = "This is a comprehensive article about machine learning."
        assert concepts["root"] == expected_sentence
        
    def test_get_available_concepts(self):
        """Test getting available concepts for a node."""
        concepts = self.generator.get_available_concepts("grandchild")
        
        assert "grandchild" in concepts
        assert "child1" in concepts
        assert "root" in concepts
        assert len(concepts) == 3
        
    def test_nonexistent_node(self):
        """Test extracting concepts from nonexistent node."""
        with pytest.raises(ValueError, match="Node 'nonexistent' does not exist"):
            self.generator.extract_concepts_from_node("nonexistent")
import pytest
import tempfile
import json
from pathlib import Path
from daydreaming_experiment.concept import Concept
from daydreaming_experiment.concept_db import ConceptDB


class TestConceptDB:
    def test_empty_db_creation(self):
        """Test creating an empty ConceptDB."""
        db = ConceptDB()
        assert len(db) == 0
        assert db.get_concepts() == []

    def test_db_creation_with_concepts(self):
        """Test creating ConceptDB with initial concepts."""
        concepts = [Concept(name="concept1"), Concept(name="concept2")]
        db = ConceptDB(concepts)
        assert len(db) == 2
        assert len(db.get_concepts()) == 2

    def test_add_concept(self):
        """Test adding concepts to database."""
        db = ConceptDB()
        concept = Concept(name="test_concept")
        db.add_concept(concept)

        assert len(db) == 1
        assert db.get_concepts()[0].name == "test_concept"

    def test_get_concept_by_name(self):
        """Test retrieving concept by name."""
        concept = Concept(
            name="neural_networks", descriptions={"sentence": "Test sentence"}
        )
        db = ConceptDB([concept])

        retrieved = db.get_concept("neural_networks")
        assert retrieved.name == "neural_networks"
        assert retrieved.sentence == "Test sentence"

    def test_get_concept_not_found(self):
        """Test retrieving non-existent concept raises KeyError."""
        db = ConceptDB()
        with pytest.raises(KeyError, match="Concept 'missing' not found"):
            db.get_concept("missing")

    def test_get_combinations_single(self):
        """Test getting 1-combinations."""
        concepts = [
            Concept(name="concept1"),
            Concept(name="concept2"),
            Concept(name="concept3"),
        ]
        db = ConceptDB(concepts)

        combinations = list(db.get_combinations(1))
        assert len(combinations) == 3
        assert all(len(combo) == 1 for combo in combinations)
        assert {combo[0].name for combo in combinations} == {
            "concept1",
            "concept2",
            "concept3",
        }

    def test_get_combinations_pairs(self):
        """Test getting 2-combinations."""
        concepts = [
            Concept(name="concept1"),
            Concept(name="concept2"),
            Concept(name="concept3"),
        ]
        db = ConceptDB(concepts)

        combinations = list(db.get_combinations(2))
        assert len(combinations) == 3  # C(3,2) = 3
        assert all(len(combo) == 2 for combo in combinations)

    def test_get_combinations_too_large(self):
        """Test getting k-combinations where k > number of concepts."""
        concepts = [Concept(name="concept1")]
        db = ConceptDB(concepts)

        combinations = list(db.get_combinations(2))
        assert len(combinations) == 0

    def test_save_and_load_minimal(self):
        """Test saving and loading database with minimal concepts."""
        concepts = [
            Concept(name="concept1", descriptions={"sentence": "Sentence 1"}),
            Concept(name="concept2", descriptions={"paragraph": "Paragraph 2"}),
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            db = ConceptDB(concepts)
            manifest_path = Path(temp_dir) / "test_manifest.json"
            db.save(str(manifest_path))

            # Check manifest was created
            assert manifest_path.exists()

            # Load and verify
            loaded_db = ConceptDB.load(str(manifest_path))
            assert len(loaded_db) == 2

            concept1 = loaded_db.get_concept("concept1")
            assert concept1.sentence == "Sentence 1"
            assert concept1.paragraph is None

            concept2 = loaded_db.get_concept("concept2")
            assert concept2.paragraph == "Paragraph 2"
            assert concept2.sentence is None

    def test_save_and_load_with_articles(self):
        """Test saving and loading database with article content."""
        concepts = [
            Concept(
                name="neural_networks",
                descriptions={
                    "sentence": "Networks mimic brains.",
                    "paragraph": "Neural networks are computational models.",
                    "article": "This is a full article about neural networks...",
                },
            )
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            db = ConceptDB(concepts)
            manifest_path = Path(temp_dir) / "test_manifest.json"
            db.save(str(manifest_path))

            # Check article file was created
            article_path = Path(temp_dir) / "articles" / "neural_networks.txt"
            assert article_path.exists()

            with open(article_path, "r") as f:
                content = f.read()
            assert content == "This is a full article about neural networks..."

            # Load and verify
            loaded_db = ConceptDB.load(str(manifest_path))
            concept = loaded_db.get_concept("neural_networks")
            assert concept.article == "This is a full article about neural networks..."
            assert concept.article_path == "articles/neural_networks.txt"

    def test_save_and_load_with_custom_articles_dir(self):
        """Test saving with custom articles directory."""
        concepts = [
            Concept(
                name="test_concept",
                descriptions={
                    "sentence": "Test sentence.",
                    "article": "Article content",
                },
            )
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            db = ConceptDB(concepts)
            manifest_path = Path(temp_dir) / "test_manifest.json"
            db.save(str(manifest_path), articles_dir="custom_articles")

            # Check custom article directory was created
            article_path = Path(temp_dir) / "custom_articles" / "test_concept.txt"
            assert article_path.exists()

            # Check manifest contains custom articles_dir
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
            assert manifest["articles_dir"] == "custom_articles"

            # Load and verify
            loaded_db = ConceptDB.load(str(manifest_path))
            concept = loaded_db.get_concept("test_concept")
            assert concept.article == "Article content"
            assert concept.article_path == "custom_articles/test_concept.txt"

    def test_save_and_load_with_existing_article_path(self):
        """Test saving and loading concepts with pre-existing article_path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create an article file manually
            articles_dir = Path(temp_dir) / "articles"
            articles_dir.mkdir()
            article_path = articles_dir / "existing_article.txt"
            with open(article_path, "w") as f:
                f.write("Existing article content")

            concepts = [
                Concept(
                    name="existing_concept",
                    descriptions={"sentence": "Has existing article."},
                    article_path="articles/existing_article.txt",
                )
            ]

            db = ConceptDB(concepts)
            manifest_path = Path(temp_dir) / "test_manifest.json"
            db.save(str(manifest_path))

            # Load and verify article content is loaded
            loaded_db = ConceptDB.load(str(manifest_path))
            concept = loaded_db.get_concept("existing_concept")
            assert concept.article == "Existing article content"
            assert concept.article_path == "articles/existing_article.txt"

    def test_load_nonexistent_article_file(self):
        """Test loading when article file doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create manifest with reference to non-existent article
            manifest = {
                "version": "1.0",
                "articles_dir": "articles",
                "concepts": [
                    {
                        "name": "missing_article",
                        "sentence": "Has missing article file.",
                        "article_path": "articles/missing.txt",
                    }
                ],
            }

            manifest_path = Path(temp_dir) / "test_manifest.json"
            with open(manifest_path, "w") as f:
                json.dump(manifest, f)

            # Should load without error, but article will be None
            loaded_db = ConceptDB.load(str(manifest_path))
            concept = loaded_db.get_concept("missing_article")
            assert concept.article is None
            assert concept.article_path == "articles/missing.txt"

    def test_manifest_format_includes_articles_dir(self):
        """Test that saved manifest includes articles_dir field."""
        concepts = [Concept(name="test", descriptions={"sentence": "Test concept."})]

        with tempfile.TemporaryDirectory() as temp_dir:
            db = ConceptDB(concepts)
            manifest_path = Path(temp_dir) / "test_manifest.json"
            db.save(str(manifest_path))

            # Check manifest format
            with open(manifest_path, "r") as f:
                manifest = json.load(f)

            assert "version" in manifest
            assert "articles_dir" in manifest
            assert "concepts" in manifest
            assert manifest["articles_dir"] == "articles"
            assert len(manifest["concepts"]) == 1

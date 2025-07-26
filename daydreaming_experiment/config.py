"""
Configuration module for the daydreaming experiment.
"""
import os

# OpenRouter API configuration
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# List of pre-June 2025 models to test
PRE_JUNE_2025_MODELS = [
    # Add model identifiers compatible with OpenRouter
    # Examples (replace with actual model identifiers):
    # "openai/gpt-3.5-turbo",
    # "anthropic/claude-2",
    # "google/palm-2-chat-bison",
    # ...
]

# Fixed seed for reproducibility
SEED = 42

# Scoring rubric for evaluating responses
SCORING_RUBRIC = {
    "random_pair_generator": "Mentions or implies a random-pair generator or similar combinatorial process",
    "critic_filter": "Mentions or implies a critic filter or evaluation mechanism for generated ideas",
    "feedback_loop": "Mentions or implies a feedback loop for storing valuable ideas for future combinations",
    "background_process": "Bonus: Mentions the background/continuous nature"
}
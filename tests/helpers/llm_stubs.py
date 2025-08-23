from dagster import ConfigurableResource


class CannedLLMResource(ConfigurableResource):
    """Mock LLM resource with canned responses for prompt types.

    - If prompt looks like links generation, returns multiline bullet points (>= 3 lines).
    - If prompt looks like evaluation, returns a paragraph plus a SCORE line.
    - Otherwise, returns an essay-like body.
    """

    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int = None) -> str:
        # Links generation first (keywords may overlap with evaluation)
        if (
            "concept link generation" in prompt.lower()
            or "link generation" in prompt.lower()
            or ("generate" in prompt.lower() and "links" in prompt.lower())
            or "candidate links" in prompt.lower()
            or "pair explorer" in prompt.lower()
        ):
            return self._get_links_response(model, prompt)
        elif "score" in prompt.lower() or "rate" in prompt.lower() or "evaluate" in prompt.lower():
            return self._get_evaluation_response(model)
        else:
            return self._get_essay_response(model, prompt)

    def get_client(self):
        return self

    def _get_links_response(self, model: str, prompt: str) -> str:
        # Minimal, still >= 3 lines
        return (
            "\n".join(
                [
                    f"• Creative concept connections using {model}",
                    "• Systematic exploration of idea combinations",
                    "• Novel insights from conceptual intersections",
                ]
            )
        )

    def _get_essay_response(self, model: str, prompt: str) -> str:
        return (
            "This synthesis reveals how structured exploration of concepts can amplify creativity.\n\n"
            "Key insights: human intuition complements systematic verification; innovation emerges from novel combinations."
        )

    def _get_evaluation_response(self, model: str) -> str:
        return (
            "Reasoning: Good creative synthesis with practical implications.\n\n"
            "SCORE: 7.5"
        )


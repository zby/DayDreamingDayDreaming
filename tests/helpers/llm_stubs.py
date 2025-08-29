from dagster import ConfigurableResource


class CannedLLMResource(ConfigurableResource):
    """Mock LLM resource with canned responses for prompt types.

    - If prompt looks like links generation, returns multiline bullet points (>= 3 lines).
    - If prompt looks like evaluation, returns a paragraph plus a SCORE line.
    - Otherwise, returns an essay-like body.
    """

    def generate(self, prompt: str, model: str, temperature: float = 0.7, max_tokens: int = None) -> str:
        # Heuristic routing:
        # - links_response always sets max_tokens explicitly in production; use that as primary signal
        if max_tokens is not None:
            return self._get_links_response(model, prompt)
        
        # Fallbacks by prompt content (useful if call sites change):
        pl = prompt.lower()
        if (
            "concept link generation" in pl
            or "link generation" in pl
            or ("generate" in pl and "links" in pl)
            or "candidate links" in pl
            or "pair explorer" in pl
            or "constructive synthesis" in pl
            or "link type menu" in pl
        ):
            return self._get_links_response(model, prompt)
        elif "score" in pl or "rate" in pl or "evaluate" in pl:
            return self._get_evaluation_response(model)
        else:
            return self._get_essay_response(model, prompt)

    def get_client(self):
        return self

    def _get_links_response(self, model: str, prompt: str) -> str:
        # Always return a simple multi-line body (template-agnostic)
        return "bla bla bla\nblas blas blas\nblablabla\n"

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

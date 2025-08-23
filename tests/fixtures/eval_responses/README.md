Curated evaluation response fixtures

This folder contains a larger set of real-world evaluation responses collected during pipeline runs.

The tests intentionally use only a minimal curated subset that cover distinct formatting patterns:
- `004.txt` (Total Score: 7/10)
- `035.txt` (SCORE: 5/10)
- `076.txt` (Total Score: 6.5/10)
- `022.txt` (Final Score on separate line -> 2/10)
- `163.txt` (Final Score: 4/10)

Keeping the set small makes tests faster and easier to maintain, while the remaining files are retained
for quick manual inspection and future additions. If you encounter a new score format in the wild, add a
new file here and extend the test mapping in `tests/test_eval_response_parser_with_fixtures.py`.


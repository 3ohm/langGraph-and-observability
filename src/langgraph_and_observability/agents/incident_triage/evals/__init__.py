"""The Incident Triage evals (spec §12.3; P21): the dataset, the evaluators and the eval test.

``dataset.py`` loads one case per incident fixture. ``evaluators.py`` holds the
evaluators, their thresholds and the scorecard. ``test_evals.py`` runs every case
through the graph and fails when a score misses its threshold: with the recorded model
in normal CI, and against Bedrock in the Evals workflow (``.github/workflows/evals.yml``).
"""

PY := .venv/bin/python

.PHONY: test sync7 compute7 report7 candidates smoke

test:
	pytest

sync7:
	$(PY) -m bsky_noise.cli sync --window 7 --use-appview --concurrency 1 --auto-degraded

compute7:
	$(PY) -m bsky_noise.cli compute --window 7 --compare-prior --summary-output output/summary.json

report7:
	$(PY) -m bsky_noise.cli report --window 7 --compare-prior --what-if-mute 5 --what-if-mute 10 --export-csv output/report.csv

candidates:
	$(PY) -m bsky_noise.cli candidates refresh
	$(PY) -m bsky_noise.cli candidates score --mode interaction --sample 75 --daily-budget 200 --concurrency 2 --use-appview --auto-degraded
	$(PY) -m bsky_noise.cli candidates shortlist --mode interaction --k 4

smoke:
	$(PY) -m bsky_noise.cli compute --window 7 --compare-prior --summary-output output/summary.json
	$(PY) -m bsky_noise.cli report --window 7 --compare-prior --what-if-mute 5 --what-if-mute 10 --export-csv output/report.csv
	$(PY) -c "import json; d=json.load(open('output/summary.json')); assert d.get('accounts'), 'accounts empty'; assert d.get('run_health_status') in {'good','degraded','partial'}, 'bad status'; print('smoke ok:', len(d['accounts']), d['run_health_status'])"

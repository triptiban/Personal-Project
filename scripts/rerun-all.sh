# scripts/run-all.sh
#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-ecommerce}"

# Adjust paths only if yours are different:
EXTRACTOR_YAML="${EXTRACTOR_YAML:-K8s/extractor-job.yaml}"
LOADER_YAML="${LOADER_YAML:-loader/loader-job.yaml}"
DBT_YAML="${DBT_YAML:-K8s/dbt-job.yaml}"
EXPORTER_YAML="${EXPORTER_YAML:-K8s/exporter-job.yaml}"

run_job () {
  local yaml="$1"
  local label="${2:-}"  # optional 'app' label for cleanup
  echo "🚀 Creating job from ${yaml}"

  # Create job and capture actual generated name
  local JOB
  JOB="$(kubectl create -n "${NS}" -f "${yaml}" -o jsonpath='{.metadata.name}')"
  echo "🆕 Created job: ${JOB}"

  echo "⏳ Waiting for pod to be ready..."
  kubectl wait -n "${NS}" --for=condition=ready pod -l "job-name=${JOB}" --timeout=180s || true

  echo "📜 Logs (Ctrl+C to stop following)..."
  kubectl logs -n "${NS}" -l "job-name=${JOB}" --follow --tail=200 || true

  echo "⏱  Waiting for completion..."
  kubectl wait -n "${NS}" --for=condition=complete "job/${JOB}" --timeout=1800s
  echo "✅ ${JOB} completed"
  echo
}

# (Optional) Clean old jobs by label to avoid clutter
cleanup_old () {
  local label="$1"
  kubectl delete job -n "${NS}" -l "app=${label}" --ignore-not-found || true
}

# Optional: tidy old jobs if you labeled them accordingly
cleanup_old extractor || true
cleanup_old loader || true
cleanup_old dbt || true
cleanup_old exporter || true

run_job "${EXTRACTOR_YAML}" extractor
run_job "${LOADER_YAML}"    loader
run_job "${DBT_YAML}"       dbt
run_job "${EXPORTER_YAML}"  exporter

echo "🎉 All steps finished successfully."

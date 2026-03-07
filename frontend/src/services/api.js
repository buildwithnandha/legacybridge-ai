import axios from 'axios'

const API_BASE = import.meta.env.VITE_API_URL || ''

const api = axios.create({
  baseURL: API_BASE,
  timeout: 30000,
})

export async function checkHealth() {
  const { data } = await api.get('/api/health')
  return data
}

export async function getPipelineStatus() {
  const { data } = await api.get('/api/pipeline/status')
  return data
}

export async function triggerReconRun() {
  const { data } = await api.post('/api/recon/run')
  return data
}

export async function getReport(runId) {
  const { data } = await api.get(`/api/recon/${runId}/report`)
  return data
}

export async function getReconHistory() {
  const { data } = await api.get('/api/recon/history')
  return data
}

export function getPdfUrl(runId) {
  return `${API_BASE}/api/recon/${runId}/pdf`
}

/**
 * Download PDF with retry logic (3 attempts, 1s delay).
 * Opens in new tab on success, returns false on failure.
 */
export async function downloadPdfWithRetry(runId, maxAttempts = 3) {
  const url = getPdfUrl(runId)

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const resp = await api.head(url)
      if (resp.status === 200) {
        window.open(url, '_blank')
        return true
      }
    } catch (err) {
      if (attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, 1000))
      }
    }
  }

  // Final fallback — just try opening it
  window.open(url, '_blank')
  return false
}

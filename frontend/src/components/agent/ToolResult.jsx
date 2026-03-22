import { CheckCircle, ShieldAlert, AlertTriangle, ShieldCheck, XCircle } from 'lucide-react'

function summarizeResult(tool, result) {
  if (!result) return { text: 'No result', severity: 'info' }
  if (result.error) return { text: `Error: ${result.error}`, severity: 'error' }

  switch (tool) {
    case 'get_schema_diff': {
      const missing = result.missing_columns?.length || 0
      const mismatches = result.type_mismatches?.length || 0
      if (missing === 0 && mismatches === 0) return { text: `${result.table} — No schema differences detected`, severity: 'healthy' }
      const parts = []
      if (missing) parts.push(`${missing} missing column${missing > 1 ? 's' : ''}`)
      if (mismatches) parts.push(`${mismatches} type mismatch${mismatches > 1 ? 'es' : ''}`)
      return { text: `${result.table} — ${parts.join(', ')} found`, severity: result.severity === 'CRITICAL' ? 'critical' : 'warning' }
    }
    case 'get_row_recon': {
      if (result.status === 'MATCH') return { text: `${result.table} — All ${result.source_count?.toLocaleString()} rows match`, severity: 'healthy' }
      return { text: `${result.table} — ${Math.abs(result.delta)} row difference (${result.delta_pct}%)`, severity: result.delta_pct > 5 ? 'critical' : 'warning', detail: `Source: ${result.source_count?.toLocaleString()} | Target: ${result.target_count?.toLocaleString()}` }
    }
    case 'get_cdc_events': {
      if (result.missed === 0) return { text: `${result.table} — All ${result.total_events?.toLocaleString()} events captured`, severity: 'healthy' }
      return { text: `${result.table} — ${result.missed?.toLocaleString()} missed events (${result.gap_rate}% gap)`, severity: 'critical', detail: `${result.total_events?.toLocaleString()} total events analyzed` }
    }
    case 'get_sample_diff':
      return { text: `${result.table}.${result.column} — ${result.total_affected?.toLocaleString()} rows affected`, severity: 'warning' }
    case 'get_pipeline_logs':
      return { text: `Pipeline: ${result.status || 'OK'}`, severity: 'info' }
    case 'classify_root_cause': {
      if (result.root_cause === 'HEALTHY') return { text: `${result.table} — Clean. No issues detected.`, severity: 'healthy', isClassification: true }
      return {
        text: `${result.root_cause}`,
        severity: result.priority === 'P1' ? 'critical' : result.priority === 'P2' ? 'warning' : 'info',
        isClassification: true,
        detail: `${result.table} | ${(result.confidence * 100).toFixed(0)}% confidence | ${result.affected_rows?.toLocaleString()} rows affected`,
      }
    }
    default:
      return { text: JSON.stringify(result).slice(0, 100), severity: 'info' }
  }
}

const SEVERITY_CONFIG = {
  critical: { icon: <ShieldAlert size={13} />, color: 'var(--critical)', bg: 'var(--critical-bg)', border: 'rgba(255,68,68,0.25)', label: 'CRITICAL' },
  warning:  { icon: <AlertTriangle size={13} />, color: 'var(--warning)', bg: 'var(--warning-bg)', border: 'rgba(255,184,0,0.25)', label: 'WARNING' },
  healthy:  { icon: <ShieldCheck size={13} />, color: 'var(--healthy)', bg: 'var(--healthy-bg)', border: 'rgba(0,214,143,0.25)', label: 'PASS' },
  info:     { icon: <CheckCircle size={13} />, color: 'var(--text-secondary)', bg: 'var(--bg-secondary)', border: 'var(--border-primary)', label: 'INFO' },
  error:    { icon: <XCircle size={13} />, color: 'var(--critical)', bg: 'var(--critical-bg)', border: 'rgba(255,68,68,0.25)', label: 'ERROR' },
}

export default function ToolResult({ tool, result }) {
  const { text, severity, detail, isClassification } = summarizeResult(tool, result)
  const config = SEVERITY_CONFIG[severity]

  return (
    <div className={`flex gap-3 fade-in ml-10 ${isClassification ? 'mb-3' : 'mb-1'}`}>
      <div className="flex-1 min-w-0">
        <div className={`rounded-lg overflow-hidden ${isClassification ? 'rounded-lg' : ''}`}
          style={{ background: config.bg, border: `1px solid ${config.border}` }}>
          <div className="flex items-start gap-2.5 px-3 py-2">
            {/* Severity icon */}
            <div className="mt-0.5 flex-shrink-0" style={{ color: config.color }}>
              {config.icon}
            </div>

            {/* Content */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2 mb-0.5">
                <span className={`text-[11px] font-semibold ${isClassification ? 'text-[12px]' : ''}`}
                  style={{ color: config.color }}>
                  {text}
                </span>
              </div>
              {detail && (
                <div className="text-[10px] font-mono" style={{ color: 'var(--text-muted)' }}>
                  {detail}
                </div>
              )}
            </div>

            {/* Badge */}
            <span className="text-[8px] uppercase tracking-wider font-bold flex-shrink-0 px-1.5 py-0.5 rounded-sm mt-0.5"
              style={{ color: config.color, background: `${config.color}15`, border: `1px solid ${config.color}25` }}>
              {config.label}
            </span>
          </div>
        </div>
      </div>
    </div>
  )
}

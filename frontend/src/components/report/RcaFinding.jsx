import { ShieldAlert, AlertTriangle, ShieldCheck, ArrowRight, Lightbulb } from 'lucide-react'

function getSeverityConfig(finding) {
  if (finding.root_cause === 'HEALTHY') return {
    bg: 'var(--healthy-bg)', border: 'var(--healthy)', color: 'var(--healthy)',
    icon: <ShieldCheck size={14} />, muted: true,
  }
  if (finding.priority === 'P1') return {
    bg: 'var(--critical-bg)', border: 'var(--critical)', color: 'var(--critical)',
    icon: <ShieldAlert size={14} />, muted: false,
  }
  if (finding.priority === 'P2') return {
    bg: 'var(--warning-bg)', border: 'var(--warning)', color: 'var(--warning)',
    icon: <AlertTriangle size={14} />, muted: false,
  }
  return {
    bg: 'var(--bg-tertiary)', border: 'var(--accent-blue)', color: 'var(--accent-blue)',
    icon: <ShieldCheck size={14} />, muted: false,
  }
}

export default function RcaFinding({ finding, index }) {
  const config = getSeverityConfig(finding)

  return (
    <div className="rounded-md slide-in-right"
      style={{
        background: config.bg,
        border: `1px solid ${config.border}`,
        borderLeft: `3px solid ${config.border}`,
        opacity: config.muted ? 0.7 : 1,
        animationDelay: `${index * 100}ms`,
        animationFillMode: 'backwards',
      }}>
      <div className="px-4 py-3">
        {/* Top row: name + priority + confidence */}
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2">
            <span style={{ color: config.color }}>{config.icon}</span>
            <span className="text-[13px] font-semibold" style={{ color: config.color }}>
              {finding.root_cause}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-bold uppercase px-2 py-0.5 rounded"
              style={{ color: config.color, background: `${config.color}20`, border: `1px solid ${config.color}40` }}>
              {finding.priority}
            </span>
            <span className="font-mono text-[11px]" style={{ color: 'var(--text-muted)' }}>
              {(finding.confidence * 100).toFixed(0)}%
            </span>
          </div>
        </div>

        {/* Middle row: table + affected rows */}
        <div className="flex items-center gap-3 mb-2">
          <span className="font-mono text-xs" style={{ color: 'var(--text-code)' }}>
            {finding.table}
          </span>
          <ArrowRight size={10} style={{ color: 'var(--text-muted)' }} />
          <span className="font-mono text-xs" style={{ color: 'var(--text-secondary)' }}>
            {finding.affected_rows?.toLocaleString()} rows affected
          </span>
        </div>

        {/* Bottom row: recommended fix */}
        {finding.recommended_fix && (
          <div className="flex items-start gap-2 rounded px-2.5 py-2"
            style={{ background: 'rgba(0,0,0,0.2)' }}>
            <Lightbulb size={12} className="mt-0.5 flex-shrink-0" style={{ color: 'var(--text-muted)' }} />
            <div>
              <span className="text-[10px] uppercase font-medium tracking-wide" style={{ color: 'var(--text-muted)' }}>
                Recommended Fix
              </span>
              <div className="text-xs mt-0.5 leading-relaxed" style={{ color: 'var(--text-secondary)' }}>
                {finding.recommended_fix}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

import { useState, useEffect } from 'react'
import { Activity, ShieldAlert, AlertTriangle, LayoutGrid, Timer } from 'lucide-react'

function useCountUp(target, duration = 2500) {
  const [value, setValue] = useState(100)
  useEffect(() => {
    if (target === null || target === undefined) return
    const start = 100
    const diff = start - target
    const startTime = Date.now()
    const tick = () => {
      const elapsed = Date.now() - startTime
      const progress = Math.min(elapsed / duration, 1)
      const eased = 1 - Math.pow(1 - progress, 3) // ease-out cubic
      setValue(Math.round(start - diff * eased))
      if (progress < 1) requestAnimationFrame(tick)
    }
    requestAnimationFrame(tick)
  }, [target, duration])
  return value
}

function getScoreStyle(score) {
  if (score <= 19) return {
    bg: 'var(--critical-bg)', border: 'var(--critical)', color: 'var(--critical)', label: 'CRITICAL', glow: 'var(--critical-glow)',
  }
  if (score <= 39) return {
    bg: '#1A1000', border: '#FF8800', color: '#FF8800', label: 'SEVERE', glow: 'rgba(255,136,0,0.15)',
  }
  if (score <= 69) return {
    bg: 'var(--warning-bg)', border: 'var(--warning)', color: 'var(--warning)', label: 'DEGRADED', glow: 'var(--warning-glow)',
  }
  return {
    bg: 'var(--healthy-bg)', border: 'var(--healthy)', color: 'var(--healthy)', label: 'HEALTHY', glow: 'var(--healthy-glow)',
  }
}

export default function ExecutiveSummary({ report, summary, tablesInvestigated = 0 }) {
  // Health score ONLY from summary (agent_complete event), never from initial report
  const health = summary?.health_score ?? null
  const critical = summary?.critical_count ?? report?.recon?.critical_count ?? 0
  const warning = summary?.warning_count ?? report?.recon?.warning_count ?? 0
  const duration = summary?.duration_seconds ?? null
  const [animated, setAnimated] = useState(false)

  const displayScore = useCountUp(health)
  const style = getScoreStyle(health !== null ? displayScore : 100)

  useEffect(() => {
    if (health !== null) {
      const timer = setTimeout(() => setAnimated(true), 2600)
      return () => clearTimeout(timer)
    }
  }, [health])

  // Show metrics row even during investigation, but hide health score card until complete
  const showScore = health !== null

  return (
    <div className="space-y-3">
      {/* Health score card — only visible after agent completes */}
      {showScore ? (
        <div className={`rounded-lg px-5 py-4 flex items-center justify-between transition-all duration-500 ${animated ? 'score-pulse' : ''}`}
          style={{
            background: style.bg,
            border: `1px solid ${style.border}`,
            boxShadow: animated ? `0 0 20px ${style.glow}` : 'none',
          }}>
          <div>
            <div className="text-[10px] uppercase font-medium tracking-widest mb-1" style={{ color: 'var(--text-muted)' }}>
              Migration Health Score
            </div>
            <div className="flex items-baseline gap-1">
              <span className="text-[48px] font-bold leading-none font-mono" style={{ color: style.color }}>
                {displayScore}
              </span>
              <span className="text-lg font-mono" style={{ color: 'var(--text-muted)' }}>/100</span>
            </div>
          </div>
          <div className="flex items-center gap-2 px-3 py-1.5 rounded"
            style={{ background: `${style.color}15`, border: `1px solid ${style.color}40` }}>
            <Activity size={14} style={{ color: style.color }} />
            <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: style.color }}>
              {style.label}
            </span>
          </div>
        </div>
      ) : (
        <div className="rounded-lg px-5 py-4 flex items-center justify-between"
          style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)' }}>
          <div>
            <div className="text-[10px] uppercase font-medium tracking-widest mb-1" style={{ color: 'var(--text-muted)' }}>
              Migration Health Score
            </div>
            <div className="flex items-baseline gap-1">
              <span className="text-[48px] font-bold leading-none font-mono" style={{ color: 'var(--text-muted)' }}>
                --
              </span>
              <span className="text-lg font-mono" style={{ color: 'var(--text-muted)' }}>/100</span>
            </div>
          </div>
          <div className="flex items-center gap-2 px-3 py-1.5 rounded"
            style={{ background: 'var(--bg-hover)', border: '1px solid var(--border-primary)' }}>
            <Activity size={14} style={{ color: 'var(--text-muted)' }} />
            <span className="text-xs font-semibold uppercase tracking-wide" style={{ color: 'var(--text-muted)' }}>
              INVESTIGATING
            </span>
          </div>
        </div>
      )}

      {/* Metric cards row */}
      <div className="grid grid-cols-4 gap-2">
        <MetricCard icon={<ShieldAlert size={13} />} label="Critical Issues" value={showScore ? critical : '--'} color={showScore ? 'var(--critical)' : 'var(--text-muted)'} />
        <MetricCard icon={<AlertTriangle size={13} />} label="Warning Issues" value={showScore ? warning : '--'} color={showScore ? 'var(--warning)' : 'var(--text-muted)'} />
        <MetricCard icon={<LayoutGrid size={13} />} label="Tables Analyzed" value={showScore ? 5 : tablesInvestigated} color="var(--accent-cyan)" />
        <MetricCard icon={<Timer size={13} />} label="Analysis Duration" value={duration ? `${duration}s` : '--'} color={duration ? 'var(--text-primary)' : 'var(--text-muted)'} />
      </div>
    </div>
  )
}

function MetricCard({ icon, label, value, color }) {
  return (
    <div className="rounded-md px-3 py-2.5"
      style={{ background: 'var(--bg-tertiary)', border: '1px solid var(--border-primary)' }}>
      <div className="flex items-center gap-1.5 mb-1.5">
        <span style={{ color: 'var(--text-muted)' }}>{icon}</span>
        <span className="text-[10px] uppercase font-medium tracking-wide" style={{ color: 'var(--text-muted)' }}>{label}</span>
      </div>
      <div className="text-[22px] font-bold font-mono" style={{ color }}>
        {value}
      </div>
    </div>
  )
}

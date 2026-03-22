import { Radio } from 'lucide-react'

export default function CdcAnalysis({ report }) {
  const tables = report?.recon?.tables
  if (!tables) return null

  return (
    <div>
      <div className="flex items-center gap-2 mb-2">
        <Radio size={13} style={{ color: 'var(--accent-cyan)' }} />
        <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: 'var(--text-secondary)' }}>
          CDC Event Analysis
        </span>
      </div>
      <div className="rounded-md overflow-hidden" style={{ border: '1px solid var(--border-primary)' }}>
        <table className="w-full text-xs">
          <thead>
            <tr style={{ background: 'var(--bg-tertiary)' }}>
              {['Table', 'Total Events', 'Captured', 'Missed', 'Gap Rate'].map(h => (
                <th key={h} className={`px-3 py-2 font-medium text-[10px] uppercase tracking-wider ${h !== 'Table' ? 'text-right' : 'text-left'}`}
                  style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Object.entries(tables).map(([name, info], i) => {
              const c = info.cdc_analysis
              if (!c) return null
              return (
                <tr key={name} style={{ background: i % 2 === 0 ? 'var(--bg-secondary)' : 'transparent' }}>
                  <td className="px-3 py-1.5 font-mono" style={{ color: 'var(--text-code)', borderBottom: '1px solid var(--border-primary)' }}>{name}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-primary)' }}>{c.total_events?.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-primary)' }}>{c.captured?.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: c.missed > 0 ? 'var(--critical)' : 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{c.missed?.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: c.gap_rate > 0 ? 'var(--critical)' : 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{c.gap_rate}%</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Gap patterns */}
      {Object.entries(tables).map(([name, info]) => {
        const patterns = info.cdc_analysis?.gap_patterns
        if (!patterns?.length) return null
        return (
          <div key={name} className="mt-2 rounded-md px-3 py-2"
            style={{ background: 'var(--critical-bg)', border: '1px solid rgba(255,68,68,0.2)' }}>
            <div className="text-[10px] font-semibold uppercase tracking-wide mb-1" style={{ color: 'var(--critical)' }}>
              {name} — Gap Patterns
            </div>
            {patterns.map((p, i) => (
              <div key={i} className="text-xs font-mono" style={{ color: 'var(--text-secondary)' }}>
                <span style={{ color: 'var(--critical)' }}>{p.reason}</span>
                <span style={{ color: 'var(--text-muted)' }}> \u2014 </span>
                <span>{p.count} events</span>
              </div>
            ))}
          </div>
        )
      })}
    </div>
  )
}

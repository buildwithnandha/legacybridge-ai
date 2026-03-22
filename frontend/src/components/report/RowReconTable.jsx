import { Table2 } from 'lucide-react'

export default function RowReconTable({ report }) {
  const tables = report?.recon?.tables
  if (!tables) return null

  return (
    <div>
      <div className="flex items-center gap-2 mb-2">
        <Table2 size={13} style={{ color: 'var(--accent-cyan)' }} />
        <span className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: 'var(--text-secondary)' }}>
          Row Reconciliation
        </span>
      </div>
      <div className="rounded-md overflow-hidden" style={{ border: '1px solid var(--border-primary)' }}>
        <table className="w-full text-xs">
          <thead>
            <tr style={{ background: 'var(--bg-tertiary)' }}>
              {['Table', 'Source', 'Target', 'Delta', 'Delta %', 'Checksum', 'Status'].map(h => (
                <th key={h} className={`px-3 py-2 font-medium text-[10px] uppercase tracking-wider ${h !== 'Table' && h !== 'Status' ? 'text-right' : 'text-left'}`}
                  style={{ color: 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Object.entries(tables).map(([name, info], i) => {
              const r = info.row_recon
              if (!r) return null
              return (
                <tr key={name} style={{ background: i % 2 === 0 ? 'var(--bg-secondary)' : 'transparent' }}>
                  <td className="px-3 py-1.5 font-mono" style={{ color: 'var(--text-code)', borderBottom: '1px solid var(--border-primary)' }}>{name}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-primary)' }}>{r.source_count?.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: 'var(--text-secondary)', borderBottom: '1px solid var(--border-primary)' }}>{r.target_count?.toLocaleString()}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: r.delta > 0 ? 'var(--critical)' : 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{r.delta}</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: r.delta_pct > 1 ? 'var(--critical)' : 'var(--text-muted)', borderBottom: '1px solid var(--border-primary)' }}>{r.delta_pct}%</td>
                  <td className="px-3 py-1.5 text-right font-mono" style={{ color: r.checksum_match ? 'var(--healthy)' : 'var(--critical)', borderBottom: '1px solid var(--border-primary)' }}>
                    {r.checksum_match ? 'MATCH' : 'MISMATCH'}
                  </td>
                  <td className="px-3 py-1.5 font-mono font-medium" style={{ color: r.status === 'MATCH' ? 'var(--healthy)' : 'var(--critical)', borderBottom: '1px solid var(--border-primary)' }}>{r.status}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
